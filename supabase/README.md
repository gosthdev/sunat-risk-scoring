# Supabase (portal de carga)

Secrets (`supabase secrets set ...`):
`SUPABASE_URL` y `SUPABASE_SERVICE_ROLE_KEY` (los inyecta Supabase), `AWS_REGION`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (usuario IAM solo `s3:PutObject` en `_inbox/*` del bucket raw),
`RAW_BUCKET` (patron `sunat-risk-scoring-raw`, ver `infra/storage/terraform.tfvars.example`),
`GITHUB_PAT` (Actions: write), `GITHUB_REPO` (`owner/repo`), `GITHUB_REF` (default `develop`),
`PIPELINE_WEBHOOK_SECRET`, `ALLOWED_ORIGINS` (origenes exactos separados por coma).

Fuentes permitidas (fuente -> mensual|sin_periodo): `python scripts/gen_fuentes_permitidas.py supabase/functions/_shared/fuentes_permitidas.json web/src/fuentes_permitidas.json`

Deploy:
```
supabase link --project-ref <ref>
supabase db push
supabase functions deploy firmar-carga confirmar actualizar-estado
```
`config.toml` fija `verify_jwt=false` solo en `actualizar-estado` (usa el secret).
