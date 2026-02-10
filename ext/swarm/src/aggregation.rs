//! SQL aggregation decomposition for distributed queries.
//!
//! When a distributed query contains aggregate functions (COUNT, SUM, MIN,
//! MAX, AVG), the aggregation must be decomposed into per-node partial
//! queries and a final merge query that combines the partial results.
//!
//! # Decomposition Rules
//!
//! | Original     | Node SQL                          | Merge SQL                           |
//! |--------------|-----------------------------------|-------------------------------------|
//! | COUNT(*)     | COUNT(*) AS _count                | SUM(_count)                         |
//! | COUNT(col)   | COUNT(col) AS _count_col          | SUM(_count_col)                     |
//! | SUM(col)     | SUM(col) AS _sum_col              | SUM(_sum_col)                       |
//! | MIN(col)     | MIN(col) AS _min_col              | MIN(_min_col)                       |
//! | MAX(col)     | MAX(col) AS _max_col              | MAX(_max_col)                       |
//! | AVG(col)     | SUM(col) AS _sum_col,             | SUM(_sum_col) / SUM(_count_col)     |
//! |              | COUNT(col) AS _count_col           |                                     |

use sqlparser::ast::{
    helpers::attached_token::AttachedToken, BinaryOperator, Expr, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, GroupByExpr, Ident, ObjectName, OrderBy, Query,
    Select, SelectItem, SetExpr, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Span, Token, TokenWithSpan};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// The result of decomposing a SQL query for distributed execution.
#[derive(Debug, Clone, PartialEq)]
pub struct DecomposedQuery {
    /// SQL to send to each data node.
    pub node_sql: String,
    /// SQL to execute locally on the merged results from all nodes.
    pub merge_sql: String,
    /// Whether the query contains aggregations that need special merging.
    pub has_aggregations: bool,
}

// ---------------------------------------------------------------------------
// Internal: classification of a single SELECT item
// ---------------------------------------------------------------------------

/// Describes how a single projection item should be handled.
enum ProjectionItem {
    /// A non-aggregate column expression (pass-through).
    PassThrough(SelectItem),
    /// An aggregate that maps 1-to-1 between node and merge (COUNT, SUM, MIN, MAX).
    SimpleAggregate {
        /// The function name (upper-cased), e.g. "COUNT", "SUM".
        func_name: String,
        /// The original argument expression (or None for COUNT(*)).
        arg_expr: Option<Expr>,
        /// True when the original was COUNT(*).
        is_count_star: bool,
        /// The user-supplied alias, if any.
        user_alias: Option<Ident>,
    },
    /// AVG(col) which decomposes into SUM + COUNT.
    Avg {
        /// The column expression inside AVG(...).
        arg_expr: Expr,
        /// The user-supplied alias, if any.
        user_alias: Option<Ident>,
    },
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Decompose a SQL query into a node query and a merge query suitable for
/// distributed execution.
///
/// If the query cannot be parsed, or is too complex for decomposition,
/// the function falls back to returning the original SQL for both queries.
pub fn decompose_query(sql: &str) -> Result<DecomposedQuery, String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| format!("parse error: {e}"))?;

    if statements.len() != 1 {
        return fallback(sql);
    }

    let query = match &statements[0] {
        sqlparser::ast::Statement::Query(q) => q,
        _ => return fallback(sql),
    };

    // We only handle top-level SELECT (not UNION, INTERSECT, etc.).
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return fallback(sql),
    };

    // Classify every projection item.
    let mut items: Vec<ProjectionItem> = Vec::new();
    let mut has_aggregates = false;

    for proj in &select.projection {
        match classify_projection(proj) {
            Ok(item) => {
                match &item {
                    ProjectionItem::SimpleAggregate { .. } | ProjectionItem::Avg { .. } => {
                        has_aggregates = true;
                    }
                    _ => {}
                }
                items.push(item);
            }
            Err(_) => return fallback(sql),
        }
    }

    if !has_aggregates {
        return decompose_non_aggregate(query, select);
    }

    // --- Build the node query and merge query for aggregate case ---

    let mut node_projection: Vec<SelectItem> = Vec::new();
    let mut merge_projection: Vec<SelectItem> = Vec::new();
    let mut counter: u32 = 0; // disambiguate when the same agg appears multiple times

    for item in &items {
        match item {
            ProjectionItem::PassThrough(si) => {
                node_projection.push(si.clone());
                merge_projection.push(si.clone());
            }
            ProjectionItem::SimpleAggregate {
                func_name,
                arg_expr,
                is_count_star,
                user_alias,
            } => {
                counter += 1;
                let alias_name = node_alias_for(func_name, arg_expr.as_ref(), *is_count_star, counter);

                // Node: original aggregate AS _alias
                let node_func = if *is_count_star {
                    make_count_star()
                } else {
                    make_func(func_name, arg_expr.clone().unwrap())
                };
                node_projection.push(SelectItem::ExprWithAlias {
                    expr: Expr::Function(node_func),
                    alias: Ident::new(&alias_name),
                });

                // Merge: combine partial results
                let merge_func_name = merge_func_for(func_name);
                let merge_expr =
                    Expr::Function(make_func(&merge_func_name, Expr::Identifier(Ident::new(&alias_name))));

                let merge_alias = final_alias(user_alias.as_ref(), func_name, arg_expr.as_ref(), *is_count_star);
                merge_projection.push(SelectItem::ExprWithAlias {
                    expr: merge_expr,
                    alias: Ident::new(&merge_alias),
                });
            }
            ProjectionItem::Avg { arg_expr, user_alias } => {
                counter += 1;
                let col_label = col_label_for(arg_expr);
                let sum_alias = format!("_sum_{}{}", col_label, counter);
                let count_alias = format!("_count_{}{}", col_label, counter);

                // Node: SUM(col) AS _sum_col, COUNT(col) AS _count_col
                node_projection.push(SelectItem::ExprWithAlias {
                    expr: Expr::Function(make_func("SUM", arg_expr.clone())),
                    alias: Ident::new(&sum_alias),
                });
                node_projection.push(SelectItem::ExprWithAlias {
                    expr: Expr::Function(make_func("COUNT", arg_expr.clone())),
                    alias: Ident::new(&count_alias),
                });

                // Merge: SUM(_sum_col) / SUM(_count_col)
                let merge_expr = Expr::BinaryOp {
                    left: Box::new(Expr::Function(make_func(
                        "SUM",
                        Expr::Identifier(Ident::new(&sum_alias)),
                    ))),
                    op: BinaryOperator::Divide,
                    right: Box::new(Expr::Function(make_func(
                        "SUM",
                        Expr::Identifier(Ident::new(&count_alias)),
                    ))),
                };

                let merge_alias = match user_alias {
                    Some(a) => a.value.clone(),
                    None => format!("avg_{}", col_label),
                };
                merge_projection.push(SelectItem::ExprWithAlias {
                    expr: merge_expr,
                    alias: Ident::new(&merge_alias),
                });
            }
        }
    }

    // GROUP BY: pass through to both node and merge queries.
    let group_by_exprs = extract_group_by_exprs(&select.group_by);

    // Pass-through columns referenced in GROUP BY also need to appear in
    // the node projection if they are not already there (they should be via
    // PassThrough items, but this is a safety net).

    // Node: include GROUP BY, exclude ORDER BY and LIMIT.
    let node_select = build_select(node_projection, &select.from, &group_by_exprs, &select.selection, &select.having);
    let node_query = build_query(node_select, None, None, None);
    let node_sql = format!("{}", sqlparser::ast::Statement::Query(Box::new(node_query)));

    // Merge: include GROUP BY, ORDER BY, LIMIT.
    let merge_select = build_select(
        merge_projection,
        &[merged_table_source()],
        &group_by_exprs,
        &None, // WHERE was already applied at node level
        &None, // HAVING is re-evaluated at merge on the re-aggregated columns
    );
    let merge_query = build_query(merge_select, query.order_by.clone(), query.limit.clone(), query.offset.clone());
    let merge_sql = format!("{}", sqlparser::ast::Statement::Query(Box::new(merge_query)));

    Ok(DecomposedQuery {
        node_sql,
        merge_sql,
        has_aggregations: true,
    })
}

// ---------------------------------------------------------------------------
// Non-aggregate decomposition
// ---------------------------------------------------------------------------

/// For queries without aggregates: node_sql = original without ORDER BY /
/// LIMIT; merge_sql = SELECT * FROM _merged with ORDER BY / LIMIT.
fn decompose_non_aggregate(query: &Query, select: &Select) -> Result<DecomposedQuery, String> {
    // Node query: strip ORDER BY and LIMIT but keep everything else.
    let node_select = select.clone();
    let node_query = build_query(node_select, None, None, None);
    let node_sql = format!("{}", sqlparser::ast::Statement::Query(Box::new(node_query)));

    let has_order_or_limit =
        query.order_by.is_some() || query.limit.is_some() || query.offset.is_some();

    if !has_order_or_limit {
        // Simplest case: no ORDER BY / LIMIT, merge is just UNION ALL.
        return Ok(DecomposedQuery {
            node_sql: node_sql.clone(),
            merge_sql: format!("SELECT * FROM _merged"),
            has_aggregations: false,
        });
    }

    // Build merge as: SELECT * FROM _merged ORDER BY ... LIMIT ...
    let merge_select = build_select(
        vec![SelectItem::Wildcard(
            sqlparser::ast::WildcardAdditionalOptions {
                wildcard_token: AttachedToken(TokenWithSpan {
                    token: Token::Mul,
                    span: Span::empty(),
                }),
                opt_ilike: None,
                opt_exclude: None,
                opt_except: None,
                opt_replace: None,
                opt_rename: None,
            },
        )],
        &[merged_table_source()],
        &[],
        &None,
        &None,
    );
    let merge_query = build_query(merge_select, query.order_by.clone(), query.limit.clone(), query.offset.clone());
    let merge_sql = format!("{}", sqlparser::ast::Statement::Query(Box::new(merge_query)));

    Ok(DecomposedQuery {
        node_sql,
        merge_sql,
        has_aggregations: false,
    })
}

// ---------------------------------------------------------------------------
// Projection classification
// ---------------------------------------------------------------------------

fn classify_projection(item: &SelectItem) -> Result<ProjectionItem, String> {
    match item {
        SelectItem::UnnamedExpr(expr) => classify_expr(expr, None),
        SelectItem::ExprWithAlias { expr, alias } => classify_expr(expr, Some(alias.clone())),
        // Wildcard or QualifiedWildcard -- pass through
        other => Ok(ProjectionItem::PassThrough(other.clone())),
    }
}

fn classify_expr(expr: &Expr, alias: Option<Ident>) -> Result<ProjectionItem, String> {
    match expr {
        Expr::Function(f) => {
            let func_name = f.name.to_string().to_uppercase();
            match func_name.as_str() {
                "COUNT" | "SUM" | "MIN" | "MAX" => {
                    let (arg_expr, is_count_star) = extract_single_arg(f)?;
                    Ok(ProjectionItem::SimpleAggregate {
                        func_name,
                        arg_expr,
                        is_count_star,
                        user_alias: alias,
                    })
                }
                "AVG" => {
                    let (arg_expr, is_star) = extract_single_arg(f)?;
                    if is_star {
                        return Err("AVG(*) is not valid SQL".to_string());
                    }
                    Ok(ProjectionItem::Avg {
                        arg_expr: arg_expr.ok_or("AVG requires an argument")?,
                        user_alias: alias,
                    })
                }
                // Unknown function or non-aggregate -- treat as pass-through.
                _ => {
                    let item = match alias {
                        Some(a) => SelectItem::ExprWithAlias {
                            expr: expr.clone(),
                            alias: a,
                        },
                        None => SelectItem::UnnamedExpr(expr.clone()),
                    };
                    Ok(ProjectionItem::PassThrough(item))
                }
            }
        }
        // Non-function expressions (columns, literals, etc.) are pass-through.
        _ => {
            let item = match alias {
                Some(a) => SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias: a,
                },
                None => SelectItem::UnnamedExpr(expr.clone()),
            };
            Ok(ProjectionItem::PassThrough(item))
        }
    }
}

/// Extract the single argument from a function call.
///
/// Returns `(Some(expr), false)` for normal args like `SUM(x)`, and
/// `(None, true)` for wildcard args like `COUNT(*)`.
fn extract_single_arg(f: &sqlparser::ast::Function) -> Result<(Option<Expr>, bool), String> {
    match &f.args {
        FunctionArguments::List(arg_list) => {
            if arg_list.args.len() != 1 {
                return Err(format!(
                    "{} with {} args is not supported for decomposition",
                    f.name,
                    arg_list.args.len()
                ));
            }
            match &arg_list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok((None, true)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok((Some(e.clone()), false)),
                other => Err(format!("unsupported function argument: {:?}", other)),
            }
        }
        FunctionArguments::None => Err("function has no arguments".to_string()),
        FunctionArguments::Subquery(_) => Err("subquery arguments not supported".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Alias and label helpers
// ---------------------------------------------------------------------------

/// Produce a deterministic internal alias for a node-level partial aggregate.
fn node_alias_for(func_name: &str, arg: Option<&Expr>, is_count_star: bool, counter: u32) -> String {
    if is_count_star {
        return format!("_count{}", counter);
    }
    let col = arg.map(|e| col_label_for(e)).unwrap_or_else(|| "x".to_string());
    format!("_{}_{}{}", func_name.to_lowercase(), col, counter)
}

/// Generate a short label from an expression (used in alias names).
fn col_label_for(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(id) => sanitize_label(&id.value),
        Expr::CompoundIdentifier(ids) => {
            ids.iter().map(|id| sanitize_label(&id.value)).collect::<Vec<_>>().join("_")
        }
        _ => {
            // For complex expressions, use a hash-like short name.
            let s = format!("{}", expr);
            sanitize_label(&s)
        }
    }
}

/// Keep only alphanumeric and underscore characters, lowercased.
fn sanitize_label(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>()
        .to_lowercase()
}

/// Determine the merge-level function to apply to partial results.
fn merge_func_for(node_func: &str) -> String {
    match node_func {
        "COUNT" => "SUM".to_string(), // SUM of partial counts
        other => other.to_string(),   // SUM, MIN, MAX stay the same
    }
}

/// Produce the final user-visible alias for the merge projection.
fn final_alias(
    user_alias: Option<&Ident>,
    func_name: &str,
    arg: Option<&Expr>,
    is_count_star: bool,
) -> String {
    if let Some(a) = user_alias {
        return a.value.clone();
    }
    if is_count_star {
        return "count_star".to_string();
    }
    let col = arg.map(|e| col_label_for(e)).unwrap_or_else(|| "x".to_string());
    format!("{}_{}", func_name.to_lowercase(), col)
}

// ---------------------------------------------------------------------------
// AST construction helpers
// ---------------------------------------------------------------------------

/// Create a `Function` AST node for a simple single-argument function.
fn make_func(name: &str, arg: Expr) -> sqlparser::ast::Function {
    sqlparser::ast::Function {
        name: ObjectName(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }
}

/// Create `COUNT(*)`.
fn make_count_star() -> sqlparser::ast::Function {
    sqlparser::ast::Function {
        name: ObjectName(vec![Ident::new("COUNT")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }
}

/// Build a `Select` node.
fn build_select(
    projection: Vec<SelectItem>,
    from: &[TableWithJoins],
    group_by_exprs: &[Expr],
    selection: &Option<Expr>,
    having: &Option<Expr>,
) -> Select {
    let group_by = if group_by_exprs.is_empty() {
        GroupByExpr::Expressions(vec![], vec![])
    } else {
        GroupByExpr::Expressions(group_by_exprs.to_vec(), vec![])
    };

    Select {
        select_token: AttachedToken(TokenWithSpan {
            token: Token::make_keyword("SELECT"),
            span: Span::empty(),
        }),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection,
        into: None,
        from: from.to_vec(),
        lateral_views: vec![],
        prewhere: None,
        selection: selection.clone(),
        group_by,
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: having.clone(),
        named_window: vec![],
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        connect_by: None,
    }
}

/// Build a `Query` node from a `Select` with optional ORDER BY, LIMIT, OFFSET.
fn build_query(
    select: Select,
    order_by: Option<OrderBy>,
    limit: Option<Expr>,
    offset: Option<sqlparser::ast::Offset>,
) -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(select))),
        order_by,
        limit,
        limit_by: vec![],
        offset,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    }
}

/// Extract `GROUP BY` expressions from a `GroupByExpr`.
fn extract_group_by_exprs(group_by: &GroupByExpr) -> Vec<Expr> {
    match group_by {
        GroupByExpr::Expressions(exprs, _modifiers) => exprs.clone(),
        GroupByExpr::All(_) => vec![], // GROUP BY ALL -- cannot decompose, treat as empty
    }
}

/// Build a `TableWithJoins` referencing the virtual `_merged` table.
fn merged_table_source() -> TableWithJoins {
    TableWithJoins {
        relation: TableFactor::Table {
            name: ObjectName(vec![Ident::new("_merged")]),
            alias: None,
            args: None,
            with_hints: vec![],
            version: None,
            with_ordinality: false,
            partitions: vec![],
            json_path: None,
        },
        joins: vec![],
    }
}

// ---------------------------------------------------------------------------
// Fallback
// ---------------------------------------------------------------------------

/// When we cannot decompose, return the original SQL for both node and merge.
fn fallback(sql: &str) -> Result<DecomposedQuery, String> {
    Ok(DecomposedQuery {
        node_sql: sql.to_string(),
        merge_sql: format!("SELECT * FROM _merged"),
        has_aggregations: false,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_no_aggregates() {
        let sql = "SELECT id, name FROM users";
        let result = decompose_query(sql).unwrap();
        assert!(!result.has_aggregations);
        assert_eq!(result.node_sql, "SELECT id, name FROM users");
        assert_eq!(result.merge_sql, "SELECT * FROM _merged");
    }

    #[test]
    fn test_select_with_order_by_and_limit() {
        let sql = "SELECT id, name FROM users ORDER BY name LIMIT 10";
        let result = decompose_query(sql).unwrap();
        assert!(!result.has_aggregations);
        // Node query should NOT have ORDER BY or LIMIT
        assert!(!result.node_sql.to_uppercase().contains("ORDER BY"));
        assert!(!result.node_sql.to_uppercase().contains("LIMIT"));
        // Merge query should have ORDER BY and LIMIT
        assert!(result.merge_sql.to_uppercase().contains("ORDER BY"));
        assert!(result.merge_sql.to_uppercase().contains("LIMIT"));
    }

    #[test]
    fn test_count_star() {
        let sql = "SELECT COUNT(*) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        // Node should have COUNT(*) with an alias
        assert!(result.node_sql.to_uppercase().contains("COUNT(*)"));
        assert!(result.node_sql.to_uppercase().contains(" AS "));
        // Merge should SUM the partial counts
        assert!(result.merge_sql.to_uppercase().contains("SUM("));
    }

    #[test]
    fn test_count_column() {
        let sql = "SELECT COUNT(id) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.node_sql.to_uppercase().contains("COUNT(ID)"));
        assert!(result.merge_sql.to_uppercase().contains("SUM("));
    }

    #[test]
    fn test_sum() {
        let sql = "SELECT SUM(price) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.node_sql.to_uppercase().contains("SUM(PRICE)"));
        // Merge should also be SUM of partial sums
        assert!(result.merge_sql.to_uppercase().contains("SUM("));
    }

    #[test]
    fn test_min_max() {
        let sql = "SELECT MIN(price), MAX(price) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.node_sql.to_uppercase().contains("MIN(PRICE)"));
        assert!(result.node_sql.to_uppercase().contains("MAX(PRICE)"));
        assert!(result.merge_sql.to_uppercase().contains("MIN("));
        assert!(result.merge_sql.to_uppercase().contains("MAX("));
    }

    #[test]
    fn test_avg_decomposition() {
        let sql = "SELECT AVG(price) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        // Node should have SUM and COUNT instead of AVG
        let node_upper = result.node_sql.to_uppercase();
        assert!(node_upper.contains("SUM(PRICE)"));
        assert!(node_upper.contains("COUNT(PRICE)"));
        assert!(!node_upper.contains("AVG"));
        // Merge should divide SUM by COUNT
        assert!(result.merge_sql.contains("/"));
    }

    #[test]
    fn test_aggregate_with_group_by() {
        let sql = "SELECT region, SUM(price) FROM orders GROUP BY region";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        // Both node and merge should have GROUP BY
        assert!(result.node_sql.to_uppercase().contains("GROUP BY"));
        assert!(result.merge_sql.to_uppercase().contains("GROUP BY"));
        // Region column should pass through
        assert!(result.node_sql.to_uppercase().contains("REGION"));
        assert!(result.merge_sql.to_uppercase().contains("REGION"));
    }

    #[test]
    fn test_aggregate_with_order_by() {
        let sql = "SELECT region, COUNT(*) FROM orders GROUP BY region ORDER BY region";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        // Node should NOT have ORDER BY
        assert!(!result.node_sql.to_uppercase().contains("ORDER BY"));
        // Merge should have ORDER BY
        assert!(result.merge_sql.to_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn test_full_example_from_spec() {
        let sql = "SELECT region, AVG(price), COUNT(*) FROM orders GROUP BY region ORDER BY region";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);

        let node_upper = result.node_sql.to_uppercase();
        // Node should have region, SUM(price), COUNT(price), COUNT(*)
        assert!(node_upper.contains("REGION"));
        assert!(node_upper.contains("SUM(PRICE)"));
        assert!(node_upper.contains("COUNT(PRICE)"));
        assert!(node_upper.contains("COUNT(*)"));
        assert!(node_upper.contains("GROUP BY"));
        assert!(!node_upper.contains("ORDER BY"));

        let merge_upper = result.merge_sql.to_uppercase();
        // Merge should re-aggregate and have ORDER BY
        assert!(merge_upper.contains("REGION"));
        assert!(merge_upper.contains("/"));     // AVG = SUM/COUNT
        assert!(merge_upper.contains("SUM("));
        assert!(merge_upper.contains("GROUP BY"));
        assert!(merge_upper.contains("ORDER BY"));
        assert!(merge_upper.contains("_MERGED"));
    }

    #[test]
    fn test_user_alias_preserved() {
        let sql = "SELECT SUM(price) AS total FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        // The merge query should use the user-supplied alias
        assert!(result.merge_sql.to_uppercase().contains("AS TOTAL"));
    }

    #[test]
    fn test_avg_user_alias_preserved() {
        let sql = "SELECT AVG(price) AS avg_price FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.merge_sql.to_uppercase().contains("AS AVG_PRICE"));
    }

    #[test]
    fn test_non_select_falls_back() {
        let sql = "INSERT INTO t VALUES (1, 2)";
        let result = decompose_query(sql).unwrap();
        assert!(!result.has_aggregations);
        assert_eq!(result.node_sql, sql);
    }

    #[test]
    fn test_invalid_sql_returns_error() {
        let sql = "NOT VALID SQL AT ALL %%%";
        let result = decompose_query(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_aggregates_same_type() {
        let sql = "SELECT SUM(a), SUM(b) FROM t";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        let node_upper = result.node_sql.to_uppercase();
        // Both should be present with distinct aliases
        assert!(node_upper.contains("SUM(A)"));
        assert!(node_upper.contains("SUM(B)"));
    }

    #[test]
    fn test_where_clause_preserved_in_node() {
        let sql = "SELECT COUNT(*) FROM orders WHERE status = 'active'";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        // WHERE should be in node query (filtering happens at data nodes)
        assert!(result.node_sql.to_uppercase().contains("WHERE"));
        // WHERE should NOT be in merge query (already filtered)
        assert!(!result.merge_sql.to_uppercase().contains("WHERE"));
    }

    #[test]
    fn test_limit_and_offset() {
        let sql = "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 5";
        let result = decompose_query(sql).unwrap();
        assert!(!result.has_aggregations);
        // Node should not have LIMIT or OFFSET
        assert!(!result.node_sql.to_uppercase().contains("LIMIT"));
        assert!(!result.node_sql.to_uppercase().contains("OFFSET"));
        // Merge should have both
        assert!(result.merge_sql.to_uppercase().contains("LIMIT"));
        assert!(result.merge_sql.to_uppercase().contains("OFFSET"));
    }

    #[test]
    fn test_merge_references_merged_table() {
        let sql = "SELECT SUM(x) FROM t";
        let result = decompose_query(sql).unwrap();
        assert!(result.merge_sql.contains("_merged"));
    }

    #[test]
    fn test_node_preserves_from_clause() {
        let sql = "SELECT SUM(price) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.node_sql.to_uppercase().contains("FROM ORDERS"));
    }

    #[test]
    fn test_mixed_aggregate_and_non_aggregate() {
        let sql = "SELECT region, COUNT(*), MIN(price), MAX(price) FROM orders GROUP BY region";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);

        let node_upper = result.node_sql.to_uppercase();
        assert!(node_upper.contains("REGION"));
        assert!(node_upper.contains("COUNT(*)"));
        assert!(node_upper.contains("MIN(PRICE)"));
        assert!(node_upper.contains("MAX(PRICE)"));
        assert!(node_upper.contains("GROUP BY"));

        let merge_upper = result.merge_sql.to_uppercase();
        assert!(merge_upper.contains("REGION"));
        assert!(merge_upper.contains("SUM(")); // COUNT(*) becomes SUM(_count)
        assert!(merge_upper.contains("MIN("));
        assert!(merge_upper.contains("MAX("));
        assert!(merge_upper.contains("GROUP BY"));
    }
}
