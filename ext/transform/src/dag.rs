use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;

/// Build a DAG and return nodes in topological order using Kahn's algorithm.
/// Returns the ordered list of node names.
pub fn topological_sort(
    nodes: &[String],
    edges: &HashMap<String, HashSet<String>>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let node_set: HashSet<&String> = nodes.iter().collect();

    // Compute in-degree for each node
    let mut in_degree: HashMap<&String, usize> = HashMap::new();
    for node in nodes {
        in_degree.insert(node, 0);
    }

    // Build adjacency list (dependency -> dependent)
    // edges maps: node -> set of nodes it depends on
    // We need: node -> set of nodes that depend on it (forward edges)
    let mut forward: HashMap<&String, Vec<&String>> = HashMap::new();
    for node in nodes {
        forward.insert(node, Vec::new());
    }

    for (node, deps) in edges {
        if !node_set.contains(node) {
            continue;
        }
        for dep in deps {
            if node_set.contains(dep) {
                forward.get_mut(dep).unwrap().push(node);
                *in_degree.get_mut(node).unwrap() += 1;
            }
        }
    }

    // Kahn's algorithm
    let mut queue: VecDeque<&String> = VecDeque::new();
    for (node, &degree) in &in_degree {
        if degree == 0 {
            queue.push_back(node);
        }
    }

    // Sort the initial queue for deterministic output
    let mut sorted_queue: Vec<&String> = queue.drain(..).collect();
    sorted_queue.sort();
    queue.extend(sorted_queue);

    let mut result = Vec::new();

    while let Some(node) = queue.pop_front() {
        result.push(node.clone());

        let mut next_nodes: Vec<&String> = Vec::new();
        for dependent in &forward[node] {
            let deg = in_degree.get_mut(dependent).unwrap();
            *deg -= 1;
            if *deg == 0 {
                next_nodes.push(dependent);
            }
        }
        next_nodes.sort();
        queue.extend(next_nodes);
    }

    if result.len() != nodes.len() {
        let missing: Vec<String> = nodes
            .iter()
            .filter(|n| !result.contains(n))
            .cloned()
            .collect();
        return Err(format!("Circular dependency detected involving: {}", missing.join(", ")).into());
    }

    Ok(result)
}

/// Given a set of changed nodes and the dependency edges, return all transitive dependents.
pub fn transitive_dependents(
    changed: &HashSet<String>,
    all_nodes: &[String],
    edges: &HashMap<String, HashSet<String>>,
) -> HashSet<String> {
    // Build reverse map: dependency -> set of dependents
    let mut reverse: HashMap<&String, Vec<&String>> = HashMap::new();
    for node in all_nodes {
        reverse.insert(node, Vec::new());
    }
    for (node, deps) in edges {
        for dep in deps {
            if let Some(dependents) = reverse.get_mut(dep) {
                dependents.push(node);
            }
        }
    }

    let mut affected: HashSet<String> = changed.clone();
    let mut queue: VecDeque<String> = changed.iter().cloned().collect();

    while let Some(node) = queue.pop_front() {
        if let Some(dependents) = reverse.get(&node) {
            for dep in dependents {
                if affected.insert((*dep).clone()) {
                    queue.push_back((*dep).clone());
                }
            }
        }
    }

    affected
}
