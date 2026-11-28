import { useEffect, useRef, useState, type DragEvent, type KeyboardEvent } from 'react'
import fuentesPermitidas from './fuentes_permitidas.json'
import { ApiError, invocar, type CargaFirmada } from './supabase'
import { fmtTamano } from './format'
import { deducirPeriodo } from '../../supabase/functions/_shared/periodo'

const MAX_ARCHIVOS = 20
const CONCURRENCIA = 3
const FUENTES: Record<string, string> = fuentesPermitidas // fuente -> 'mensual' | 'sin_periodo'

type Firma = { lote_id: string; cargas: (CargaFirmada & { nombre_original: string })[]; omitidos?: string[] }

interface Item {
  id: string
  file: File
  carga: CargaFirmada
  estado: 'cola' | 'subiendo' | 'ok' | 'error'
  pct: number
  error?: string
}

function periodos() {
  const hoy = new Date()
  return Array.from({ length: 36 }, (_, i) => {
    const d = new Date(hoy.getFullYear(), hoy.getMonth() - i, 1)
    const v = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`
    return { v, t: d.toLocaleDateString('es-PE', { month: 'long', year: 'numeric' }) }
  })
}
const PERIODOS = periodos()
const FUENTE_POR_PREFIJO: [string, string][] = [['PADRONRUC', 'padron_ruc'], ['ORDENESCOMPRA', 'ordenes_compra']]

// POST prefirmado: todos los fields y `file` al final
function subir(c: CargaFirmada, file: File, onPct: (p: number) => void) {
  return new Promise<void>((ok, ko) => {
    const fd = new FormData()
    Object.entries(c.fields).forEach(([k, v]) => fd.append(k, v))
    fd.append('file', file)
    const xhr = new XMLHttpRequest()
    xhr.open('POST', c.url)
    xhr.upload.onprogress = (e) => e.lengthComputable && onPct(Math.round((e.loaded / e.total) * 100))
    xhr.onload = () => (xhr.status >= 200 && xhr.status < 300 ? ok() : ko(new ApiError(`Falló la subida (HTTP ${xhr.status}).`, xhr.status)))
    xhr.onerror = () => ko(new Error('Falló la conexión durante la subida.'))
    xhr.send(fd)
  })
}

export default function Upload({ onEnviado, irAHistorial }: { onEnviado: () => void; irAHistorial: () => void }) {
  const [fuente, setFuente] = useState('')
  const [manuales, setManuales] = useState<Record<string, string>>({}) // periodo elegido a mano por nombre de archivo
  const [files, setFiles] = useState<File[]>([])
  const [items, setItems] = useState<Item[]>([])
  const [busy, setBusy] = useState(false)
  const ocupado = useRef(false) // guarda síncrona contra doble clic
  const [msg, setMsg] = useState<string | null>(null)
  const [arrastrando, setArrastrando] = useState(false)
  const input = useRef<HTMLInputElement>(null)
  const envio = useRef({ lote: '', fuente: '', periodos: {} as Record<string, string> })
  const porConfirmar = useRef<string[]>([]) // subidas aún sin confirmar
  const [confirmarFallo, setConfirmarFallo] = useState(false)
  const [iniciado, setIniciado] = useState(0)
  const titulo = useRef<HTMLHeadingElement>(null)

  useEffect(() => { if (iniciado > 0) titulo.current?.focus() }, [iniciado])

  const sinPeriodo = FUENTES[fuente] === 'sin_periodo'
  const periodoDe = (f: File) => (sinPeriodo ? '-' : deducirPeriodo(f.name) ?? manuales[f.name] ?? '')
  const patch = (id: string, p: Partial<Item>) => setItems((l) => l.map((i) => (i.id === id ? { ...i, ...p } : i)))

  const agregar = (nuevos: FileList | File[]) => {
    const lista = [...files]
    let aviso: string | null = null
    for (const f of nuevos) {
      if (lista.some((x) => x.name === f.name)) aviso = `Ya elegiste un archivo llamado ${f.name}. Renombra uno de los dos.`
      else if (lista.length >= MAX_ARCHIVOS) aviso = `Máximo ${MAX_ARCHIVOS} archivos por envío.`
      else lista.push(f)
    }
    if (!fuente && !files.length && lista.length) {
      const n = lista[0].name.toUpperCase()
      const p = FUENTE_POR_PREFIJO.find(([pre]) => n.startsWith(pre))
      if (p) setFuente(p[1])
    }
    setFiles(lista)
    setMsg(aviso)
  }
  const soltar = (e: DragEvent) => { e.preventDefault(); setArrastrando(false); agregar(e.dataTransfer.files) }
  const teclado = (e: KeyboardEvent) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); input.current?.click() } }

  // Vuelve a firmar un solo archivo. null = el servidor lo omitió (nada que re-firmar).
  const firmarUno = async (file: File): Promise<CargaFirmada | null> => {
    const { lote, fuente, periodos } = envio.current
    let r: Firma
    try {
      r = await invocar<Firma>('firmar-carga', { fuente, lote_id: lote, archivos: [{ nombre: file.name, tamano: file.size, periodo: periodos[file.name] }] })
    } catch (e) {
      if (e instanceof ApiError && e.cuerpo?.omitidos?.includes(file.name)) return null
      throw e
    }
    if (r.omitidos?.includes(file.name)) return null
    const c = r.cargas.length === 1 && r.cargas[0].nombre_original === file.name ? r.cargas[0] : null
    if (!c) throw new Error('La respuesta no coincide con el archivo a reintentar.')
    return c
  }

  const confirmar = async () => {
    const ids = [...porConfirmar.current]
    if (!ids.length) return
    try {
      await invocar('confirmar', { lote_id: envio.current.lote, carga_ids: ids })
      porConfirmar.current = porConfirmar.current.filter((i) => !ids.includes(i))
      setConfirmarFallo(false)
    } catch (e) {
      setConfirmarFallo(true)
      setMsg(`No se pudo confirmar el envío: ${(e as Error).message}`)
    } finally {
      onEnviado()
    }
  }

  // Sube con 3 en paralelo; al terminar confirma lo subido
  const correr = async (lista: Item[]) => {
    const { lote } = envio.current
    let n = 0
    const worker = async () => {
      while (n < lista.length) {
        const it = lista[n++]
        patch(it.id, { estado: 'subiendo', pct: 0, error: undefined })
        let carga = it.carga
        try {
          try {
            await subir(carga, it.file, (pct) => patch(it.id, { pct }))
          } catch (e) {
            if (!(e instanceof ApiError && e.status === 403)) throw e
            // política vencida: se vuelve a firmar una sola vez
            const nueva = await firmarUno(it.file)
            if (!nueva) throw new Error('El servidor ya no espera este archivo.')
            carga = nueva
            patch(it.id, { carga })
            await subir(carga, it.file, (pct) => patch(it.id, { pct }))
          }
          patch(it.id, { estado: 'ok', pct: 100 })
          porConfirmar.current.push(carga.carga_id)
        } catch (e) {
          patch(it.id, { estado: 'error', error: (e as Error).message })
          await invocar('firmar-carga', { lote_id: lote, marcar_error: [carga.carga_id] }).catch(() => {})
        }
      }
    }
    await Promise.all(Array.from({ length: Math.min(CONCURRENCIA, lista.length) }, worker))
    await confirmar()
  }

  const enviar = async () => {
    if (ocupado.current || items.length > 0) return
    ocupado.current = true
    if (new Set(files.map((f) => f.name)).size !== files.length) { setMsg('Hay archivos con el mismo nombre. Renombra uno.'); return }
    setBusy(true)
    setMsg(null)
    try {
      const r = await invocar<Firma>('firmar-carga', {
        fuente, archivos: files.map((f) => ({ nombre: f.name, tamano: f.size, periodo: periodoDe(f) })),
      })
      const porNombre = new Map(r.cargas.map((c) => [c.nombre_original, c]))
      const lista: Item[] = files.map((file) => {
        const carga = porNombre.get(file.name)
        if (!carga) throw new Error(`El servidor no devolvió firma para ${file.name}.`)
        return { id: crypto.randomUUID(), file, carga, estado: 'cola', pct: 0 }
      })
      envio.current = { lote: r.lote_id, fuente, periodos: Object.fromEntries(files.map((f) => [f.name, periodoDe(f)])) }
      porConfirmar.current = []
      setItems(lista)
      setFiles([])
      setIniciado((n) => n + 1)
      await correr(lista)
    } catch (e) {
      setMsg((e as Error).message)
    } finally {
      setBusy(false)
      ocupado.current = false
    }
  }

  const reintentar = async (it: Item) => {
    if (ocupado.current) return
    ocupado.current = true
    setBusy(true)
    setMsg(null)
    try {
      const carga = await firmarUno(it.file)
      if (!carga) {
        // el servidor ya la tiene: no hay nada que subir ni confirmar
        patch(it.id, { estado: 'ok', pct: 100, error: undefined })
        setMsg(`${it.file.name} ya estaba recibido; revisa el historial.`)
        onEnviado()
        return
      }
      patch(it.id, { carga, estado: 'cola', error: undefined })
      await correr([{ ...it, carga }])
    } catch (e) {
      setMsg((e as Error).message)
    } finally {
      setBusy(false)
      ocupado.current = false
    }
  }

  const reintentarConfirmar = async () => {
    if (ocupado.current) return
    ocupado.current = true
    setBusy(true)
    setMsg(null)
    await confirmar()
    setBusy(false)
    ocupado.current = false
  }

  const enCurso = items.some((i) => i.estado === 'cola' || i.estado === 'subiendo')
  const terminado = items.length > 0 && !enCurso && !busy

  return (
    <main>
      <h2 style={{ marginTop: 20 }}>Nuevo envío</h2>
      <div className="stack">
        <label>
          Fuente
          <select value={fuente} onChange={(e) => setFuente(e.target.value)} disabled={busy || items.length > 0}>
            <option value="" disabled>Elige una fuente</option>
            {Object.keys(FUENTES).map((f) => <option key={f} value={f}>{f}</option>)}
          </select>
        </label>
        <p className="muted small" style={{ margin: 0 }}>Si ya enviaste archivos para este periodo, este envío los reemplaza.</p>

        {items.length === 0 && (
          <>
            <div
              className={`drop${arrastrando ? ' over' : ''}`}
              role="button" tabIndex={0}
              aria-label="Elegir archivos. También puedes soltarlos aquí."
              onClick={() => input.current?.click()} onKeyDown={teclado}
              onDragOver={(e) => { e.preventDefault(); setArrastrando(true) }}
              onDragLeave={() => setArrastrando(false)} onDrop={soltar}
            >
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M12 16V4m0 0L7.5 8.5M12 4l4.5 4.5M4 15v3a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-3" /></svg>
              <strong>{arrastrando ? 'Suéltalos para agregarlos' : 'Suelta archivos aquí o presiona para elegirlos'}</strong>
              <span className="muted small">CSV o Excel, hasta {MAX_ARCHIVOS} archivos por envío.</span>
            </div>
            <input ref={input} type="file" multiple hidden onChange={(e) => { agregar(e.target.files ?? []); e.target.value = '' }} />
            {files.length > 0 && (
              <ul className="files">
                {files.map((f, i) => (
                  <li key={`${f.name}-${i}`}>
                    <span className="name">{f.name}</span>
                    <span className="num">{fmtTamano(f.size)}</span>
                    {FUENTES[fuente] === 'mensual' && (deducirPeriodo(f.name)
                      ? <span className="chip num">Periodo {deducirPeriodo(f.name)} detectado</span>
                      : (
                        <select className="small" aria-label={`Periodo de ${f.name}`} value={manuales[f.name] ?? ''} disabled={busy}
                          onChange={(e) => setManuales({ ...manuales, [f.name]: e.target.value })}>
                          <option value="" disabled>Elige periodo</option>
                          {PERIODOS.map((p) => <option key={p.v} value={p.v}>{p.t}</option>)}
                        </select>
                      ))}
                    <button className="link" onClick={() => setFiles(files.filter((_, j) => j !== i))} aria-label={`Quitar ${f.name}`}>Quitar</button>
                  </li>
                ))}
              </ul>
            )}
            <button className="primary" disabled={busy || !fuente || files.length === 0 || files.some((f) => !periodoDe(f))} onClick={enviar}>
              {busy ? 'Enviando…' : 'Enviar al pipeline'}
            </button>
          </>
        )}
      </div>

      {msg && <p role="alert" className="err aviso">{msg}</p>}

      {items.length > 0 && <h2 ref={titulo} tabIndex={-1}>Envío en curso</h2>}
      {items.length > 0 && (
        <ul className="files" aria-live="polite">
          {items.map((i) => (
            <li key={i.id}>
              <span className="name">{i.file.name}</span>
              <span className="num">{fmtTamano(i.file.size)}</span>
              {i.estado === 'error' ? (
                <>
                  <span className="err small detalle">{i.error}</span>
                  <button aria-disabled={busy} onClick={() => reintentar(i)}>Reintentar</button>
                </>
              ) : (
                <>
                  <span className={`estado small num${i.estado === 'ok' ? ' ok' : ''}`} aria-hidden="true">
                    {i.estado === 'ok' ? 'Subido' : i.estado === 'subiendo' ? `Subiendo ${i.pct} %` : 'En cola'}
                  </span>
                  <progress max={100} value={i.pct} aria-label={`Progreso de ${i.file.name}`} />
                </>
              )}
            </li>
          ))}
        </ul>
      )}

      {terminado && (
        <p className="row" style={{ marginTop: 16 }}>
          {confirmarFallo && <button className="primary" onClick={reintentarConfirmar}>Reintentar envío</button>}
          <button className={confirmarFallo ? '' : 'primary'} onClick={irAHistorial}>Ver historial</button>
          <button disabled={confirmarFallo} onClick={() => { setItems([]); setFuente(''); setManuales({}); setMsg(null) }}>Nuevo envío</button>
        </p>
      )}
    </main>
  )
}
