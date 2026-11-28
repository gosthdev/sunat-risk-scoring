import { useCallback, useEffect, useRef, useState } from 'react'
import type { User } from '@supabase/supabase-js'
import { supabase, type Carga } from './supabase'
import Login from './Login'
import Upload from './Upload'
import History from './History'

const LIMITE = 500

// Timestamps comparables como texto: separador T, fracción a 6 dígitos, sufijo UTC unificado
const norm = (s: string) =>
  s.replace(' ', 'T').replace(/(\.\d+)?(Z|\+00(:?00)?)?$/, (_m, f: string | undefined) => `${(f ?? '.').padEnd(7, '0')}Z`)

export default function App() {
  const [user, setUser] = useState<User | null>(null)
  const [listo, setListo] = useState(false)
  const [vista, setVista] = useState<'enviar' | 'historial'>('enviar')
  const [cargas, setCargas] = useState<Carga[]>([])
  const [sellados, setSellados] = useState<ReadonlySet<string>>(new Set()) // lotes con UPDATE en vivo
  const [errorCarga, setErrorCarga] = useState<string | null>(null)
  const [aviso, setAviso] = useState<string | null>(null)
  const vistos = useRef(new Map<string, string>()) // id -> actualizado_en más reciente visto por Realtime
  const uid = user?.id

  // Evita que el navegador abra un archivo soltado fuera de la zona (también en el login)
  useEffect(() => {
    const parar = (e: Event) => e.preventDefault()
    window.addEventListener('dragover', parar)
    window.addEventListener('drop', parar)
    return () => { window.removeEventListener('dragover', parar); window.removeEventListener('drop', parar) }
  }, [])

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => { setUser(data.session?.user ?? null); setListo(true) })
    const { data } = supabase.auth.onAuthStateChange((_e, s) => setUser(s?.user ?? null))
    return () => data.subscription.unsubscribe()
  }, [])

  const recargar = useCallback(async () => {
    const { data, error } = await supabase.from('cargas').select('*').order('creado_en', { ascending: false }).limit(LIMITE)
    if (error) { setErrorCarga(error.message); return }
    setErrorCarga(null)
    // Una fila más vieja que el último evento Realtime de esa fila no pisa al evento
    setCargas((prev) => {
      const previas = new Map(prev.map((c) => [c.id, c]))
      return (data as Carga[]).map((r) => {
        const visto = vistos.current.get(r.id)
        return visto && r.actualizado_en && norm(r.actualizado_en) < visto && previas.has(r.id) ? previas.get(r.id)! : r
      })
    })
  }, [])

  useEffect(() => {
    if (!uid) { setCargas([]); return }
    const canal = supabase
      .channel('cargas')
      .on('postgres_changes', { event: 'UPDATE', schema: 'public', table: 'cargas', filter: `user_id=eq.${uid}` }, (p) => {
        const nueva = p.new as Carga
        if (nueva.actualizado_en) vistos.current.set(nueva.id, norm(nueva.actualizado_en))
        // github_run_url no viene garantizado en el payload; se conserva el valor previo
        setCargas((prev) => prev.map((c) => (c.id === nueva.id ? { ...c, ...nueva } : c)))
        setSellados((s) => new Set(s).add(nueva.lote_id))
      })
      .subscribe((estado) => {
        if (estado === 'SUBSCRIBED') { setAviso(null); recargar() } // recupera lo ocurrido antes/durante la conexión
        else if (estado === 'CHANNEL_ERROR' || estado === 'TIMED_OUT') setAviso('Se perdió la conexión en vivo. Los estados pueden estar desactualizados; recarga la página.')
      })
    return () => { supabase.removeChannel(canal) }
  }, [uid, recargar])

  if (!listo) return null
  if (!user) return <Login />

  return (
    <div className="page">
      <header className="top">
        <div className="brand"><span className="marca" aria-hidden="true">MP</span><h1>Mesa de partes</h1></div>
        <button className="link" onClick={() => supabase.auth.signOut()}>Salir</button>
      </header>
      <nav className="tabs" aria-label="Secciones">
        <button aria-current={vista === 'enviar' ? 'page' : undefined} onClick={() => setVista('enviar')}>Enviar</button>
        <button aria-current={vista === 'historial' ? 'page' : undefined} onClick={() => setVista('historial')}>Historial</button>
      </nav>
      {aviso && <p role="status" className="err aviso">{aviso}</p>}
      {/* Upload sigue montado (oculto) para no perder una subida en curso */}
      <div hidden={vista !== 'enviar'}>
        <Upload onEnviado={recargar} irAHistorial={() => setVista('historial')} />
      </div>
      {vista === 'historial' && (
        <History cargas={cargas} error={errorCarga} limite={LIMITE} admin={user.app_metadata?.rol === 'admin'} sellados={sellados} irAEnviar={() => setVista('enviar')} />
      )}
    </div>
  )
}
