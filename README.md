# Cloneless Grands Automation

Automates the full pipeline for your club workflow:

1. Fetch Weekly Grands campaign(s).
2. Download source map(s).
3. Strip `RaceValidateGhost` using the bundled pure-Python map transformer.
4. Rewrite map UID to variant-specific cloneless UIDs.
5. Generate lap variants (default: `1, 2, 3, 5, 10, 20, 30, 60, 120, 256`).
6. Scale Author Time from the source lap count and auto-derive Gold/Silver/Bronze using the same formula as `script-tm_Gbx-Medal-Time-Modifier`.
7. Update the embedded map name and thumbnail so each variant visibly includes its lap count.
8. Upload maps to Nadeo Core (`POST /maps/` multipart).
9. Create or update the club campaign with the full uploaded playlist.
10. Activate/publicize the campaign activity.
11. Upload activity media (thumbnail/render) for the campaign card.
12. Enforce activity ordering (`Information` pinned first, processed campaign right after).
13. Upload club background from latest weekly render image.
14. Optionally add the map UIDs to a map-upload bucket.

Bundled local tools (so no external absolute paths are required):
- `tools/strip-validation/stripValidationReplay.exe` (legacy fallback)
- `tools/strip-validation/gbxlzo.exe` (legacy fallback)
- `tools/gbx-json-editor/GbxJsonEditor.Cli.exe` (legacy fallback)
- `src/gbxpy/*` (vendored pure-Python GBX parser used by default)

## Setup

Project layout:

- `src/` -> Python app
- `scripts/` -> PowerShell runners/scheduler helpers
- root -> config JSON + dotenv files
- `docs/` -> extra notes/patch files

1. Create and activate a venv (optional).
2. Install deps:

```powershell
pip install -r requirements.txt
```

3. Copy config and edit:

```powershell
Copy-Item config.example.json config.json
```

`config.example.json` defaults to the bundled pure-Python transformer. The older exe-based strip/rewrite path is still available via `map.transform_mode = "legacy"`.

Default lap-variant config:

- `map.lap_variants = [1, 2, 3, 5, 10, 20, 30, 60, 120, 256]`
- `map.variant_name_template = "{map_name_base} {lap_count}L"`
- Variant output filenames include the lap tag as `{lap_count:03d}L`
- Variant map thumbnails get a lap-count overlay badge during generation

Author Time is scaled from the source map's real lap count, and Gold/Silver/Bronze are auto-generated with the same formula used by `st-AR-gazer/script-tm_Gbx-Medal-Time-Modifier`:

- Gold = `AT x 1.06`, rounded up to the next whole second
- Silver = `AT x 1.20`, rounded up to the next whole second
- Bronze = `AT x 1.50`, rounded up to the next whole second

4. Create dotenv file:

```powershell
Copy-Item .env.example .env
```

Then edit `.env`:

```env
TM_SERVICE_ACCOUNT_LOGIN=your_service_account_login
TM_SERVICE_ACCOUNT_PASSWORD=your_service_account_password
```

Create the service account at `https://www.trackmania.com/player/service-account`.
The default auth mode now uses Trackmania service-account credentials via Nadeo's `/v2/authentication/token/basic` endpoint.
If you ever need the older Ubisoft login flow, set `auth.mode` to `"ubisoft"` and provide `UBI_EMAIL` / `UBI_PASSWORD` instead.

By default the script auto-loads `.env` (see `env` section in `config.json`).

## Run

```powershell
python src\cloneless_grands.py --config config.json
```

Useful options:

- `--dry-run`: shows what would happen without uploads/edits.
- `--force`: ignores processed state and re-runs the current source campaign.
- `--offset N`: override Weekly Grands offset.
- `--length N`: process multiple campaigns from latest backward.

## Task Scheduler (Windows)

Quick run wrapper:

```powershell
.\scripts\run_cloneless_grands.ps1 -ConfigPath .\config.json
```

Compliance check wrapper:

```powershell
.\scripts\check_cloneless_grands_compliance.ps1 -ConfigPath .\config.json
```

Install weekly scheduled task:

```powershell
.\scripts\install_weekly_task.ps1 -TaskName "ClonelessGrandsWeekly" -Day MON -Time 18:00 -ConfigPath .\config.json -PythonExe python
```

Create and run immediately once:

```powershell
.\scripts\install_weekly_task.ps1 -TaskName "ClonelessGrandsWeekly" -Day MON -Time 18:00 -ConfigPath .\config.json -PythonExe python -RunNow
```

## UID Rewrite Modes

`config.json -> map.uid_rewriter.mode`:

- `internal_replace` (default): replaces all occurrences of source UID bytes with new UID bytes in the stripped map file.
- `external_command`: runs your custom command using placeholders:
  - `{input}`
  - `{output}`
  - `{old_uid}`
  - `{new_uid}`

Bundled external-command setup in this repo:

- JSON template: `tools/gbx-json-editor/uid-rewrite.instructions.template.json`
- Wrapper: `tools/gbx-json-editor/rewrite_uid_with_gbxjsoneditor.ps1`
- Config command template (already set in `config.example.json`):

```json
"uid_rewriter": {
  "mode": "external_command",
  "command_template": "powershell -NoProfile -ExecutionPolicy Bypass -File \"tools/gbx-json-editor/rewrite_uid_with_gbxjsoneditor.ps1\" -InputPath \"{input}\" -OutputPath \"{output}\" -NewUid \"{new_uid}\""
}
```

Example:

```json
"uid_rewriter": {
  "mode": "external_command",
  "command_template": "C:/path/to/your/uid_tool.exe \"{input}\" \"{output}\" \"{new_uid}\""
}
```

## Notes

- Campaign names are capped at 20 chars by the API. The script truncates if `campaign.truncate_to_20 = true`.
- Default campaign naming uses `w{week:02d} {source_map_name_clean}`.
- Default variant map naming uses `{map_name_base} {lap_count}L`.
- State is stored in `work/state.json` to avoid reposting the same `seasonUid`.
- `work/raw` and `work/processed` keep downloaded and transformed map files.
- Dotenv loading is configurable via `env.load_dotenv`, `env.dotenv_path`, and `env.override_existing_env`.
- Activity thumbnail upload is automated: the campaign card prefers the official Weekly Grand media, falls back to the clean source thumbnail, and only then falls back to map imagery; bytes are posted directly to `.../activity/{activityId}/upload` (not JSON/multipart path mode).
- Activity ordering can be enforced via `ordering.*` config (default: pin `Information` news at `0`, move processed campaign to `1`).
- Club background upload is automated: raw bytes are posted to `.../club/{clubId}/media/upload?format=background`, preferring latest Weekly Grands `campaign.mediaUrl`.
