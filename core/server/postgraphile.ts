import { postgraphile } from "postgraphile";
import { PostGraphileAmberPreset } from "postgraphile/presets/amber";
import { makePgService } from "postgraphile/adaptors/pg";
import { PostGraphileConnectionFilterPreset } from "postgraphile-plugin-connection-filter";

export function createPostGraphile(databaseUrl: string, schemas: string[]) {
  return postgraphile({
    extends: [PostGraphileAmberPreset, PostGraphileConnectionFilterPreset],
    pgServices: [
      makePgService({
        connectionString: databaseUrl,
        schemas,
      }),
    ],
    grafserv: {
      graphqlPath: "/graphql",
      graphiqlPath: "/graphiql",
      graphiql: true,
    },
    grafast: {
      context(ctx: any) {
        const req = ctx.req || ctx.request;
        return {
          pgSettings: req?.pgSettings || {},
        };
      },
    },
  });
}
