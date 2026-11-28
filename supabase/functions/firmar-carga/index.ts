import { createPresignedPost } from "npm:@aws-sdk/s3-presigned-post@3";
import { S3Client } from "npm:@aws-sdk/client-s3@3";
import { admin, esUuid, FUENTES, handler, json, usuario, validarFuentePeriodo } from "../_shared/lib.ts";
import { deducirPeriodo } from "../_shared/periodo.ts";

const MAX = 5 * 1024 ** 3;
const s3 = new S3Client({ region: Deno.env.get("AWS_REGION") });

const firmar = (fuente: string, periodo: string, id: string, archivo: string, tamano: number) =>
  createPresignedPost(s3, {
    Bucket: Deno.env.get("RAW_BUCKET")!,
    Key: `_inbox/${fuente}/${periodo}/${id}/${archivo}`,
    Conditions: [["content-length-range", tamano, tamano]],
    Expires: 3600, // las colas pueden demorar la subida
  });

Deno.serve(handler(async (req, c) => {
  const user = await usuario(req);
  if (!user) return json({ error: "no autenticado" }, 401, c);
  const b = await req.json().catch(() => null);
  if (!b) return json({ error: "body invalido" }, 400, c);
  const db = admin();

  // Marcar fallos de subida: solo filas propias en 'firmada'.
  if (b.marcar_error !== undefined) {
    if (!esUuid(b.lote_id) || !Array.isArray(b.marcar_error) || !b.marcar_error.length || !b.marcar_error.every(esUuid)) {
      return json({ error: "ids invalidos" }, 400, c);
    }
    const { error } = await db.from("cargas").update({ estado: "error_subida" })
      .in("id", b.marcar_error).eq("lote_id", b.lote_id).eq("user_id", user.id).eq("estado", "firmada");
    return error ? json({ error: "interno" }, 500, c) : json({ ok: true }, 200, c);
  }

  const { fuente, archivos } = b;
  if (typeof fuente !== "string" || !/^[a-z0-9_]+$/.test(fuente)) return json({ error: "fuente invalida" }, 400, c);
  if (!Array.isArray(archivos) || archivos.length < 1 || archivos.length > 20) return json({ error: "archivos: 1..20" }, 400, c);
  if (b.lote_id !== undefined && !esUuid(b.lote_id)) return json({ error: "lote_id invalido" }, 400, c);

  const items: { original: string; nombre: string; tamano: number; periodo: string }[] = [];
  for (const a of archivos) {
    const original = String(a?.nombre ?? "");
    const nombre = original.replace(/[^A-Za-z0-9._-]/g, "_");
    if (nombre.includes("..") || !/\.(csv|xlsx)$/i.test(nombre)) return json({ error: `archivo invalido: ${original}` }, 400, c);
    if (!Number.isInteger(a.tamano) || a.tamano <= 0 || a.tamano > MAX) return json({ error: `tamano invalido: ${original}` }, 400, c);
    let periodo = "-";
    if (FUENTES[fuente] === "mensual") {
      const ded = deducirPeriodo(original);
      const cliente = a.periodo ?? b.periodo;
      if (ded && cliente && cliente !== ded) return json({ error: `El periodo enviado no coincide con el de ${original} (${ded}).` }, 400, c);
      periodo = ded ?? cliente;
      if (!/^[0-9]{4}-(0[1-9]|1[0-2])$/.test(String(periodo))) return json({ error: `No se pudo deducir el periodo de ${original}; elígelo manualmente.` }, 400, c);
    }
    const errFP = validarFuentePeriodo(fuente, periodo, c);
    if (errFP) return errFP;
    items.push({ original, nombre, tamano: a.tamano, periodo });
  }
  if (new Set(items.map((i) => i.nombre)).size !== items.length) return json({ error: "nombres duplicados tras sanitizar" }, 400, c);

  let lote_id: string;
  let filas: { id: string; item: (typeof items)[number] }[];
  let omitidos: string[] = [];
  if (b.lote_id) {
    lote_id = b.lote_id;
    const { data, error } = await db.from("cargas").select("id, archivo, periodo")
      .eq("lote_id", lote_id).eq("user_id", user.id).eq("fuente", fuente)
      .in("periodo", [...new Set(items.map((i) => i.periodo))])
      .in("estado", ["firmada", "error_subida"]).in("archivo", items.map((i) => i.nombre));
    if (error) throw error;
    filas = [];
    for (const i of items) {
      const r = data!.find((d) => d.archivo === i.nombre && d.periodo === i.periodo);
      if (r) filas.push({ id: r.id, item: i }); else omitidos.push(i.original);
    }
    // el tamano debe coincidir con la condicion de S3: se actualiza al del cliente; solo se firman filas realmente actualizadas
    const hechas: typeof filas = [];
    for (const f of filas) {
      const { data: u, error: e } = await db.from("cargas")
        .update({ estado: "firmada", detalle_error: null, tamano: f.item.tamano })
        .eq("id", f.id).eq("user_id", user.id).eq("lote_id", lote_id).in("estado", ["firmada", "error_subida"]).select("id");
      if (e) throw e;
      if (u.length) hechas.push(f); else omitidos.push(f.item.original);
    }
    filas = hechas;
    if (!filas.length) return json({ error: "nada que re-firmar", omitidos }, 404, c);
  } else {
    // shortcut: counted on cargas table, move to Upstash if traffic grows
    const desde = new Date(Date.now() - 10 * 60_000).toISOString();
    const { data, error } = await db.from("cargas").select("lote_id").eq("user_id", user.id).gte("creado_en", desde);
    if (error) throw error;
    if (new Set(data!.map((r) => r.lote_id)).size >= 10) return json({ error: "demasiadas cargas, reintenta luego" }, 429, c);
    lote_id = crypto.randomUUID();
    filas = items.map((item) => ({ id: crypto.randomUUID(), item }));
  }

  // Firmar antes de insertar: si falla, no quedan filas huerfanas.
  const cargas = await Promise.all(filas.map(async ({ id, item }) => {
    const { url, fields } = await firmar(fuente, item.periodo, id, item.nombre, item.tamano);
    return { carga_id: id, nombre: item.nombre, nombre_original: item.original, url, fields };
  }));

  if (!b.lote_id) {
    const { error } = await db.from("cargas").insert(filas.map(({ id, item }) => ({
      id, lote_id, user_id: user.id, fuente, periodo: item.periodo, archivo: item.nombre, tamano: item.tamano, estado: "firmada",
    })));
    if (error) throw error;
  }
  return json({ lote_id, cargas, omitidos }, 200, c);
}));
