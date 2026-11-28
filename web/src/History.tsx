import { useMemo } from 'react'
import type { Carga, Estado } from './supabase'
import { fmtFecha, fmtTamano } from './format'

const ETAPAS = ['recibido', 'intake', 'pendientes', 'bronze', 'silver', 'completado']
const ETAPA: Record<Estado, number> = {
  firmada: 0, error_subida: 0, subida: 0, encolada: 0, intake: 1, pendientes: 2,
  bronze: 3, silver: 4, completada: 5, rechazada: -1, duplicada: -1, error: -1,
}
const TEXTO: Record<Estado, string> = {
  firmada: 'Firmada', error_subida: 'Error de subida', subida: 'Subida', encolada: 'En cola', intake: 'Intake',
  pendientes: 'Pendiente', bronze: 'Bronze', silver: 'Silver', completada: 'Completada',
  rechazada: 'Rechazada', duplicada: 'Duplicada', error: 'Error',
}
const tono = (e: Estado) =>
  e === 'completada' ? 'ok' : e === 'rechazada' || e === 'duplicada' ? 'obs' : e === 'error' || e === 'error_subida' ? 'err' : ''

// La etapa del expediente es la del archivo menos avanzado que sigue en curso
function etapaLote(cs: Carga[]) {
  const vivos = cs.filter((c) => c.estado !== 'error_subida').map((c) => ETAPA[c.estado]).filter((n) => n >= 0)
  return vivos.length ? Math.min(...vivos) : -1
}

// Sello visible del expediente: estado del archivo con peor situación, o la etapa vigente
function Sello({ cs, sellar }: { cs: Carga[]; sellar: boolean }) {
  const malo = cs.find((c) => tono(c.estado) === 'err') ?? cs.find((c) => tono(c.estado) === 'obs')
  const e = etapaLote(cs)
  const t = malo ? tono(malo.estado) : cs.every((c) => c.estado === 'completada') ? 'ok' : ''
  const texto = malo ? TEXTO[malo.estado] : e >= 0 ? ETAPAS[e] : 'Sin etapa'
  return <span key={texto} className={`sello-badge ${t}${sellar ? ' sello' : ''}`} aria-hidden="true">{texto}</span>
}

function Pista({ etapa, sellar }: { etapa: number; sellar: boolean }) {
  return (
    <>
    <p className="etapa-txt muted" aria-hidden="true">{etapa >= 0 ? `Etapa ${etapa + 1} de ${ETAPAS.length}: ${ETAPAS[etapa]}` : 'Sin etapa vigente'}</p>
    <ol className="pista" aria-label={etapa >= 0 ? `Etapa actual: ${ETAPAS[etapa]}` : 'Sin etapa vigente'}>
      {ETAPAS.map((e, i) => (
        <li key={i === etapa ? `${e}-actual` : e} className={i < etapa || (i === etapa && etapa === 5) ? 'hecho' : i === etapa ? `actual${sellar ? ' sello' : ''}` : ''}
          aria-current={i === etapa ? 'step' : undefined}>
          <span>{e}</span>
        </li>
      ))}
    </ol>
    </>
  )
}

export default function History({ cargas, error, limite, admin, sellados, irAEnviar }: { cargas: Carga[]; error: string | null; limite: number; admin: boolean; sellados: ReadonlySet<string>; irAEnviar: () => void }) {
  const lotes = useMemo(() => {
    const m = new Map<string, Carga[]>()
    for (const c of cargas) m.set(c.lote_id, [...(m.get(c.lote_id) ?? []), c])
    return [...m.values()]
  }, [cargas])

  if (!lotes.length) {
    return (
      <main>
        {error ? <p role="alert" className="err aviso">No se pudo cargar el historial: {error}. Recarga la página para reintentar.</p> : (
          <div className="vacio">
            <h2 style={{ marginTop: 0 }}>Aún no hay envíos</h2>
            <p className="muted">Cuando envíes archivos, cada envío aparecerá aquí con su etapa en el pipeline.</p>
            <button className="primary" onClick={irAEnviar}>Nuevo envío</button>
          </div>
        )}
      </main>
    )
  }

  return (
    <main>
      <h2>Historial</h2>
      {error && <p role="alert" className="err aviso">No se pudo actualizar el historial: {error}</p>}
      {cargas.length >= limite && <p className="muted small">Mostrando los {limite} más recientes.</p>}
      {lotes.map((cs) => {
        const { fuente, periodo, creado_en, lote_id } = cs[0]
        return (
          <section key={lote_id} className="expediente">
            <header>
              <div>
                <h3>{fuente} <span className="muted num">{periodo === '-' ? 'Sin periodo' : periodo}</span></h3>
                <p className="muted small num meta">{fmtFecha(creado_en)} · {cs.length} {cs.length === 1 ? 'archivo' : 'archivos'}</p>
              </div>
              <Sello cs={cs} sellar={sellados.has(lote_id)} />
            </header>
            <Pista etapa={etapaLote(cs)} sellar={sellados.has(lote_id)} />
            <ul className="files">
              {cs.map((c) => (
                <li key={c.id}>
                  <span className="name">{c.archivo}</span>
                  <span className="num">{fmtTamano(c.tamano)}</span>
                  <span className={`estado ${tono(c.estado)}`}>{TEXTO[c.estado]}</span>
                  {c.detalle_error && <span className="detalle">{c.detalle_error}</span>}
                  {admin && c.github_run_url?.startsWith('https://github.com/') && <a href={c.github_run_url} target="_blank" rel="noreferrer">Ver ejecución</a>}
                </li>
              ))}
            </ul>
          </section>
        )
      })}
    </main>
  )
}
