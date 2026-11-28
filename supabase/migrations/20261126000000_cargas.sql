create table public.cargas (
  id uuid primary key default gen_random_uuid(),
  lote_id uuid not null,
  user_id uuid not null references auth.users on delete cascade,
  fuente text,
  periodo text,
  archivo text,
  tamano bigint,
  estado text not null check (estado in (
    'firmada','error_subida','subida','encolada','intake','pendientes',
    'bronze','silver','completada','rechazada','duplicada','error')),
  detalle_error text,
  github_run_url text,
  creado_en timestamptz default now(),
  actualizado_en timestamptz default now()
);

create index cargas_user_creado_idx on public.cargas (user_id, creado_en);
create index cargas_lote_idx on public.cargas (lote_id);

alter table public.cargas enable row level security;

create policy cargas_select_own on public.cargas
  for select using (user_id = (select auth.uid()));

create function public.cargas_bump_actualizado() returns trigger
language plpgsql set search_path = '' as $$
begin
  new.actualizado_en = now();
  return new;
end $$;

create trigger cargas_bump_actualizado before update on public.cargas
  for each row execute function public.cargas_bump_actualizado();

alter publication supabase_realtime add table public.cargas;
