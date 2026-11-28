import { createClient } from "npm:@supabase/supabase-js@2";
import fuentes from "./fuentes_permitidas.json" with { type: "json" };

export const FUENTES: Record<string, string> = fuentes;
export const ESTADOS = [
  "firmada", "error_subida", "subida", "encolada", "intake", "pendientes",
  "bronze", "silver", "completada", "rechazada", "duplicada", "error",
] as const;

export const admin = () =>
  createClient(Deno.env.get("SUPABASE_URL")!, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!);

export function cors(req: Request): Record<string, string> {
  const allowed = (Deno.env.get("ALLOWED_ORIGINS") ?? "").split(",").map((s) => s.trim()).filter(Boolean);
  const origin = req.headers.get("Origin") ?? "";
  return {
    ...(origin && allowed.includes(origin) ? { "Access-Control-Allow-Origin": origin } : {}),
    "Access-Control-Allow-Headers": "authorization, apikey, content-type, x-client-info",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Vary": "Origin",
  };
}

export function json(body: unknown, status = 200, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...headers, "Content-Type": "application/json" },
  });
}

/** Maneja OPTIONS/POST y errores; h recibe (req, headers CORS). */
export function handler(h: (req: Request, c: Record<string, string>) => Promise<Response>) {
  return async (req: Request) => {
    const c = cors(req);
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: c });
    if (req.method !== "POST") return json({ error: "metodo" }, 405, c);
    try {
      return await h(req, c);
    } catch (e) {
      console.error(e);
      return json({ error: "interno" }, 500, c);
    }
  };
}

/** Usuario del JWT (la plataforma ya verificó la firma; aquí se resuelve el usuario). */
export async function usuario(req: Request) {
  const jwt = (req.headers.get("Authorization") ?? "").replace(/^Bearer /, "");
  const { data } = await admin().auth.getUser(jwt);
  return data.user;
}

export const esUuid = (s: unknown): s is string =>
  typeof s === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s);

/** Valida fuente y periodo contra el mapa de fuentes; devuelve la respuesta de error o null. */
export function validarFuentePeriodo(fuente: string, periodo: unknown, c: Record<string, string>): Response | null {
  const tipo = Object.hasOwn(FUENTES, fuente) ? FUENTES[fuente] : undefined;
  if (!tipo) return json({ error: "fuente no permitida" }, 403, c);
  if (tipo === "mensual" && !(typeof periodo === "string" && /^[0-9]{4}-(0[1-9]|1[0-2])$/.test(periodo))) {
    return json({ error: "Esta fuente es mensual: el periodo debe tener formato AAAA-MM." }, 400, c);
  }
  if (tipo === "sin_periodo" && periodo !== "-") {
    return json({ error: "Esta fuente no usa periodo: debe enviarse \"-\"." }, 400, c);
  }
  return null;
}
