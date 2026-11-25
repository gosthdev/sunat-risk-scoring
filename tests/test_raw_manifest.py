"""Decisión incremental de raw con manifest. Sin red ni AWS reales."""

import json
import subprocess

import pytest

from src.ingestion import process_and_upload as pu

URLS = {
    "padron_ruc": {"2025-01": "http://x/p1.zip", "2025-02": "http://x/p2.zip"},
    "small_datasets": {"see": "http://x/see.zip"},
}


class FakeS3:
    """Doble de `aws`: guarda objetos en memoria y registra llamadas."""

    def __init__(self):
        self.objs: dict[str, bytes] = {}
        self.calls: list[list[str]] = []

    def __call__(self, args, check=True):
        self.calls.append(list(args))
        a = args
        if a[:2] == ["s3", "cp"] and a[3] == "-":
            if a[2] in self.objs:
                return subprocess.CompletedProcess(a, 0, self.objs[a[2]].decode(), "")
            return subprocess.CompletedProcess(
                a, 1, "", "An error occurred (404) Not Found"
            )
        if a[:2] == ["s3", "cp"]:
            with open(a[2], "rb") as f:
                self.objs[a[3]] = f.read()
        if a[:2] == ["s3", "ls"]:
            ok = a[2] in self.objs
            return subprocess.CompletedProcess(a, 0 if ok else 1, "x" if ok else "", "")
        return subprocess.CompletedProcess(a, 0, "", "")


@pytest.fixture
def env(tmp_path, monkeypatch):
    s3 = FakeS3()
    net: dict = {"head": [], "downloads": [], "head_resp": (304, {}), "content": b"v1"}

    def head(url, headers=None):
        net["head"].append((url, headers))
        r = net["head_resp"]
        if isinstance(r, Exception):
            raise r
        return r

    def handler(periodo, url):
        net["downloads"].append(url)
        f = tmp_path / "padron_ruc" / f"{periodo}.csv"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(net["content"])
        return [f], url

    monkeypatch.setattr(pu, "BASE_DIR", tmp_path)
    monkeypatch.setattr(pu, "_aws", s3)
    monkeypatch.setattr(pu, "http_head", head)
    monkeypatch.setitem(pu.HANDLERS, "padron_ruc", handler)
    monkeypatch.setitem(pu.HANDLERS, "see", lambda p, u: handler("see", u))
    monkeypatch.setattr(pu, "FUENTES_FILE", tmp_path / "no.json")
    monkeypatch.setattr(pu, "URLS_FILE", tmp_path / "urls.json")
    (tmp_path / "urls.json").write_text(json.dumps(URLS))
    monkeypatch.delenv("FORCE_REDOWNLOAD", raising=False)
    return s3, net


def entry(sha="a" * 64, etag=None, lm=None):
    return {
        "sha256": sha,
        "etag": etag,
        "last_modified": lm,
        "url": "http://x/p1.zip",
        "ingested_at": "t",
    }


def run(
    manifest, periodo="2025-01", force=False, fuente="padron_ruc", url="http://x/p1.zip"
):
    pu.process_item(fuente, periodo, url, manifest, force, set(), set())


def test_collect_items_flattens_small_datasets():
    assert ("see", "-", "http://x/see.zip") in pu.collect_items(URLS)
    assert all(f != "small_datasets" for f, _, _ in pu.collect_items(URLS))


def test_registered_without_validators_is_immutable_no_network(env):
    _, net = env
    m = {"padron_ruc": {"2025-01": entry()}}
    run(m)
    assert net["head"] == [] and net["downloads"] == []


def test_head_304_skips(env):
    _, net = env
    m = {"padron_ruc": {"2025-01": entry(etag='"e1"')}}
    run(m)
    assert net["head"][0][1] == {"If-None-Match": '"e1"'}
    assert net["downloads"] == []


def test_head_200_same_sha_updates_validators_and_does_not_upload(env, tmp_path):
    _, net = env
    sha = pu.hashlib.sha256(b"v1").hexdigest()
    m = {"padron_ruc": {"2025-01": entry(sha=sha, etag='"e1"')}}
    net["head_resp"] = (200, {"etag": '"e2"', "last-modified": "Tue"})
    run(m)
    e = m["padron_ruc"]["2025-01"]
    assert net["downloads"] and e["etag"] == '"e2"' and e["last_modified"] == "Tue"
    assert e["sha256"] == sha and e["ingested_at"] == "t"
    assert not (tmp_path / "padron_ruc" / "2025-01.csv").exists()  # nada que subir


def test_new_sha_updates_entry(env):
    _, net = env
    m = {"padron_ruc": {"2025-01": entry(etag='"e1"')}}
    net["head_resp"] = (200, {"etag": '"e2"'})
    run(m)
    assert m["padron_ruc"]["2025-01"]["sha256"] == pu.hashlib.sha256(b"v1").hexdigest()


def test_new_period_downloads_and_seals_sha(env):
    _, net = env
    net["head_resp"] = (200, {"etag": '"n"', "last-modified": "Mon"})
    m: dict = {}
    run(m, "2025-02", url="http://x/p2.zip")
    e = m["padron_ruc"]["2025-02"]
    assert e["sha256"] == pu.hashlib.sha256(b"v1").hexdigest()
    assert (e["etag"], e["last_modified"], e["url"]) == (
        '"n"',
        "Mon",
        "http://x/p2.zip",
    )


def test_head_error_warns_and_skips(env, capsys):
    _, net = env
    net["head_resp"] = OSError("boom")
    m = {"padron_ruc": {"2025-01": entry(etag='"e1"')}}
    run(m)
    assert net["downloads"] == [] and "WARN" in capsys.readouterr().out


def test_force_redownloads_everything(env):
    _, net = env
    m = {"padron_ruc": {"2025-01": entry()}}
    run(m, force=True)
    assert net["downloads"] == ["http://x/p1.zip"]
    assert all(h is None for _, h in net["head"])  # sin HEAD condicional


def test_duplicate_sha_warns(env, capsys):
    sha = pu.hashlib.sha256(b"v1").hexdigest()
    m = {"padron_ruc": {"2025-01": entry(sha=sha)}}
    run(m, "2025-02", url="http://x/p2.zip")
    assert "mismo archivo que padron_ruc/2025-01" in capsys.readouterr().out


def test_bootstrap_hashes_existing_objects_without_validators(env, monkeypatch):
    _s3, net = env
    monkeypatch.setattr(
        pu,
        "s3_sha256",
        lambda uri: (
            "h-" + uri.rsplit("/", 1)[-1] if "mes=01" in uri or "see" in uri else None
        ),
    )
    m = pu.bootstrap_manifest(pu.collect_items(URLS))
    assert m["padron_ruc"]["2025-01"]["sha256"] == "h-padron.csv"
    assert "2025-02" not in m["padron_ruc"]
    assert m["padron_ruc"]["2025-01"]["etag"] is None and m["see"]["-"]["sha256"]
    # y tras el bootstrap no hay red
    run(m)
    assert net["head"] == [] and net["downloads"] == []


def test_main_merge_keeps_removed_periods_and_orders_writes(env, capsys):
    s3, _net = env
    old = {
        "padron_ruc": {"2024-12": entry(), "2025-01": entry(), "2025-02": entry()},
        "see": {"-": entry()},
    }
    s3.objs[f"s3://{pu.RAW_BUCKET}/{pu.MANIFEST_KEY}"] = json.dumps(old).encode()
    pu.main()
    saved = json.loads(s3.objs[f"s3://{pu.RAW_BUCKET}/{pu.MANIFEST_KEY}"])
    assert "2024-12" in saved["padron_ruc"]
    assert "padron_ruc/2024-12" in capsys.readouterr().out
    cps = [c[3] for c in s3.calls if c[:2] == ["s3", "cp"]]
    i_manifest = cps.index(f"s3://{pu.RAW_BUCKET}/{pu.MANIFEST_KEY}")
    i_ok = cps.index(f"s3://{pu.RAW_BUCKET}/_INGESTION_SUCCESS")
    assert i_manifest < i_ok
    assert s3.objs[f"s3://{pu.RAW_BUCKET}/_runs/last_run_pendientes.json"] == b"[]"


def test_load_manifest_error_is_not_treated_as_absent(env, monkeypatch):
    monkeypatch.setattr(
        pu,
        "_aws",
        lambda a, check=True: subprocess.CompletedProcess(a, 255, "", "AccessDenied"),
    )
    with pytest.raises(RuntimeError):
        pu.load_manifest()
