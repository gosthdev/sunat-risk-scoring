import { admin, esUuid, handler, json, usuario } from "../_shared/lib.ts";

Deno.serve(handler(async (req, c) => {
  const user = await usuario(req);
  if (!user) return json({ error: "no autenticado" }, 401, c);
  const b = await req.json().catch(() => null);
  if (!esUuid(b?.lote_id) || !Array.isArray(b.carga_ids) || !b.carga_ids.length || !b.carga_ids.every(esUuid)) {
    return json({ error: "body invalido" }, 400, c);
  }
  const db = admin();
  const { data, error } = await db.from("cargas").update({ estado: "encolada" })
    .in("id", b.carga_ids).eq("lote_id", b.lote_id).eq("user_id", user.id).eq("estado", "firmada").select("id");
  if (error) throw error;
  // shortcut: no S3 HeadObject check here, the pipeline rejects missing files; add if users confirm without uploading
  if (!data.length) return json({ encoladas: 0 }, 200, c); // idempotente: ya confirmadas

  const r = await fetch(
    `https://api.github.com/repos/${Deno.env.get("GITHUB_REPO")}/actions/workflows/pipeline.yml/dispatches`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${Deno.env.get("GITHUB_PAT")}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "sunat-portal",
      },
      body: JSON.stringify({ ref: Deno.env.get("GITHUB_REF") || "develop" }),
    },
  ).catch(() => null);
  if (!r?.ok) {
    const { error: e2 } = await db.from("cargas").update({ estado: "firmada", detalle_error: `dispatch fallido (${r?.status ?? "red"}), reintenta confirmar` })
      .in("id", data.map((d) => d.id));
    if (e2) {
      console.error(e2);
      return json({ error: "interno" }, 500, c);
    }
    return json({ error: "no se pudo iniciar el pipeline" }, 502, c);
  }
  return json({ encoladas: data.length }, 200, c);
}));
