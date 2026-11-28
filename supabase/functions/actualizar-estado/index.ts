import { timingSafeEqual } from "node:crypto";
import { admin, ESTADOS, esUuid, json } from "../_shared/lib.ts";

// Sin CORS: lo llama GitHub Actions, no el navegador.
const sha = async (t: string) => new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(t)));
const igual = async (a: string, b: string) => timingSafeEqual(await sha(a), await sha(b));

Deno.serve(async (req) => {
  if (req.method !== "POST") return json({ error: "metodo" }, 405);
  const secret = Deno.env.get("PIPELINE_WEBHOOK_SECRET") ?? "";
  const token = (req.headers.get("Authorization") ?? "").replace(/^Bearer /, "");
  if (!secret || !(await igual(token, secret))) return json({ error: "no autorizado" }, 401);

  const b = await req.json().catch(() => null);
  if (!Array.isArray(b?.carga_ids) || !b.carga_ids.length || !b.carga_ids.every(esUuid) ||
      !(ESTADOS as readonly string[]).includes(b.estado)) return json({ error: "body invalido" }, 400);
  const cambios: Record<string, string> = { estado: b.estado };
  if (b.detalle_error !== undefined) {
    if (typeof b.detalle_error !== "string" || b.detalle_error.length > 2000) return json({ error: "detalle_error invalido" }, 400);
    cambios.detalle_error = b.detalle_error;
  }
  if (b.github_run_url !== undefined) {
    if (typeof b.github_run_url !== "string" || !b.github_run_url.startsWith("https://github.com/")) return json({ error: "github_run_url invalida" }, 400);
    cambios.github_run_url = b.github_run_url;
  }

  const { error } = await admin().from("cargas").update(cambios).in("id", b.carga_ids);
  return error ? json({ error: "interno" }, 500) : json({ ok: true });
});
