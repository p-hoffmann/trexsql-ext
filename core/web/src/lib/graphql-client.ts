import { type ReactNode, createElement } from "react";
import { Client, Provider, cacheExchange, fetchExchange } from "urql";

export const client = new Client({
  url: "/graphql",
  exchanges: [cacheExchange, fetchExchange],
  fetchOptions: () => ({
    credentials: "include" as const,
    method: "POST",
  }),
  preferGetMethod: false,
});

export function GraphQLProvider({ children }: { children: ReactNode }) {
  return createElement(Provider, { value: client }, children);
}
