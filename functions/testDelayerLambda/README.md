# TestDelayerLambda

Lambda (Node 20) utile per eseguire test automatici sull'algoritmo di pianificazione.

La lambda utilizza un dispatcher per supportare più tipi di operazioni utili per il testing.

## Operazioni disponibili

| Nome | Descrizione | Parametri (`event.parameters`) |
|------|-------------|--------------------------------|
| **IMPORT_DATA** | Importa un CSV da S3 nella tabella `pn-DelayerPaperDelivery` tramite scritture `BatchWrite`. | _Nessuno_ → passare un array vuoto `[]` |
| **GET_USED_CAPACITY** | Legge la capacità utilizzata per la combinazione `unifiedDeliveryDriver~geoKey` alla `deliveryDate` indicata, dalla tabella `pn-PaperDeliveryDriverUsedCapacities`. | `[ "unifiedDeliveryDriver", "geoKey", "deliveryDate (ISO‑8601 UTC)" ]` |

### Esempi di payload

*IMPORT_DATA*

```json
{
  "operationType": "IMPORT_DATA",
  "parameters": []
}
```

*GET_USED_CAPACITY*

```json
{
  "operationType": "GET_USED_CAPACITY",
  "parameters": ["Sailpost", "87100", "2025-06-30T00:00:00Z"]
}
```

### Output GET_USED_CAPACITY

* Item trovato → oggetto completo, ad esempio:
  ```json
  {
    "unifiedDeliveryDriverGeokey": "Sailpost~87100",
    "deliveryDate": "2025-06-30T00:00:00Z",
    "geoKey": "87100",
    "unifiedDeliveryDriver": "Sailpost",
    "usedCapacity": 572,
    "capacity": 1000
  }
  ```
* Item assente → `{ "message": "Item not found" }`


> Aggiungi nuove operazioni creando un nuovo modulo e registrandolo in `eventHandler.js` dentro l’oggetto `OPERATIONS`.

## Struttura del progetto

```
.
├── index.js               # Entrypoint Lambda
├── package.json           # Dipendenze e script
├── src/
│   └── app/
│       ├── eventHandler.js     # Dispatcher delle operazioni
│       ├── getUsedCapacity.js  # Implementazione operazione GET_USED_CAPACITY
│       ├── importData.js       # Implementazione operazione IMPORT_DATA
│   └── test/
│       ├── eventHandler.test.js # Test unitari (Nyc + aws-sdk-client-mock)
│       └── sample.csv     # Fixture di esempio
└── README.md
```


## Requisiti

* **Node.js 20.x** (runtime Lambda)
* **npm 9.x**

## Variabili d’ambiente

| Nome           | Descrizione                                 | Obbligatoria |
|----------------|---------------------------------------------|--------------|
| `BUCKET_NAME`  | Bucket S3 contenente il CSV                 | ✅ |
| `OBJECT_KEY`   | Key/Path dell’oggetto CSV                   | ✅ |

## Setup locale
Dalla cartella testDelayerLambda

```bash
# Installa dipendenze
npm install

# Esegui test unitari
npm test
