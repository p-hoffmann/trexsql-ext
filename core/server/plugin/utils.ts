export async function waitfor(url: string): Promise<string> {
  let reachable = false;
  while (!reachable) {
    try {
      await fetch(url);
      reachable = true;
    } catch (_e) {
      console.log(`${url} not reachable. waiting ...`);
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }
  return "OK";
}
