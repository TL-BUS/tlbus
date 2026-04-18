# About TL-Bus

TL-Bus nasce da un'idea semplice: tenere visibile il bus.

Invece di nascondere routing e metadata dentro un framework grande e opaco, il progetto espone direttamente i pezzi importanti:

- gli envelope portano routing e metadata di trace
- i plugin aggiungono lineage, auth, HMAC e comportamento protocol
- i manifest descrivono cosa puo' fare un servizio
- bridge tiene separata la delivery federata da quella locale

## Principi di design

| Principio | Significato |
| --- | --- |
| Routing esplicito | I nomi servizio si risolvono in socket o target di pool concreti |
| Tracciabilita` | `txn_id` resta visibile da ingress fino alla reply |
| Componenti piccoli | Daemon, bridge, sidecar e sender hanno ognuno un ruolo stretto |
| Testabilita` | Il workspace include test unitari e workflow CI |

## Focus del progetto

Il progetto e` centrato sul percorso del messaggio, non sulla cerimonia del framework.
TL-Bus e` quindi adatto quando vuoi:

- controllare le decisioni di routing
- osservare header e lineage dei messaggi
- comporre delivery locale e federata
- agganciare policy senza riscrivere il bus core
