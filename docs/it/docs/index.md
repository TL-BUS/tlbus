# Documentazione TL-Bus

TL-Bus e` un bus messaggi in Rust per microservizi. Mantiene espliciti routing, lineage dei messaggi e federation tra pool, cosi` il sistema resta comprensibile mentre cresce.

Questa docs segue il pattern di FastAPI: una struttura per lingua, una landing page iniziale e poche sezioni mirate sotto.

## Parti da qui

- [About](about/index.md)
- [Tutorial](tutorial/index.md)
- [Reference](reference/index.md)
- [Resources](resources/index.md)

## Su cosa si concentra TL-Bus

- envelope e header espliciti
- propagazione di `txn_id` end to end
- service manifest con capabilities e modes
- delivery locale via Unix socket
- delivery tra pool tramite bridge e sidecar

## Dove guardare nel repo

- [README](../../../README.md)
- [README italiano](../../../READMEIT.md)
- [Licenza MIT](../../../LICENSE)

