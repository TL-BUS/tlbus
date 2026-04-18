# Tutorial

Questo percorso breve mostra il progetto dall'esterno verso il core.

## 1. Esegui i test

```bash
cargo test --workspace --all-targets
```

## 2. Guarda l'help del client

```bash
cargo run -p tlbus-client -- --help
```

## 3. Invia un envelope

```bash
cargo run -p tlbus-client -- send \
  --bus-socket /run/tlb.sock \
  --service-name ps1.client \
  --service-secret shared-secret \
  --to ps1.echo \
  --payload "{\"message\":\"hello\"}"
```

## 4. Parti dai manifest

La discovery dei servizi e` guidata dai manifest. Un servizio descrive capability e modes, e il plugin protocol puo' rispondere alle richieste `*.manifest` usando quei dati.

## 5. Prova la federation

Usa sidecar e bridge quando un messaggio deve uscire dal pool locale.
In questo modo delivery locale e delivery cross-pool restano separate, e il flusso e` piu` semplice da ragionare.

## 6. Build container base

Usa una di queste build locali:

```bash
docker build -f Dockerfile-client -t tlbus-client:local .
docker build -f Dockerfile-py -t tlbus-pyclient:local .
docker build -f Dockerfile-worker -t tlbus-worker:local .
```
