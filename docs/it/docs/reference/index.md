# Reference

## Binary

| Binary | Scopo |
| --- | --- |
| `tlbus-send` | Invia un envelope a un socket del daemon |
| `tlbus-daemon` | Valida, instrada e consegna gli envelope |
| `tlbus-bridge` | Inoltra traffico federato tra pool |
| `tlbus-sidecar` | Avvia insieme daemon e bridge per un pool |

## Header core

| Header | Significato |
| --- | --- |
| `txn_id` | Correlazione della transazione lungo tutto il percorso |
| `reply_to` | Indirizzo di ritorno per reply e response di protocollo |
| `protocol` | Nome logico del protocollo nell'envelope |
| `transport` | Trasporto usato dalla delivery mode scelta |
| `content_type` | Tipo del payload quando il protocollo lo richiede |

## Variabili d'ambiente utili

| Variabile | Ruolo |
| --- | --- |
| `TLBUS_PLUGINS` | Seleziona la pipeline di plugin attiva |
| `TLB_SERVICE_SECRET` | Abilita l'handshake di registrazione servizio |
| `TLB_POOL_SECRET` | Abilita l'handshake tra pool federati |
| `TLB_HMAC_KEY` | Abilita la firma HMAC |
| `TLB_BUS_MODE` | Seleziona la modalita` sidecar |
| `TLB_BUS_POOL` | Nome del pool locale |

## Modello messaggi

- `Envelope` contiene payload e metadata
- `ServiceManifest` descrive le capability del servizio
- `ServiceDescriptor` e` la vista registrata di un servizio
- `PoolManifest` aggrega i servizi disponibili in un pool

