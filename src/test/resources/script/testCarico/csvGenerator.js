const fs = require('fs');
const path = require('path');

// Definizione delle regioni italiane con province e CAP
const regioni = {
    'Abruzzo': [['AQ', ['67100', '67039', '67051']], ['CH', ['66100', '66034', '66041']], ['PE', ['65100', '65121', '65129']], ['TE', ['64100', '64011', '64020']]],
    'Basilicata': [['MT', ['75100', '75011', '75019']], ['PZ', ['85100', '85020', '85030']]],
    'Calabria': [['CS', ['87100', '87011', '87020']], ['CZ', ['88100', '88021', '88030']], ['KR', ['88900', '88811', '88820']], ['RC', ['89100', '89011', '89020']], ['VV', ['89900', '89801', '89810']]],
    'Campania': [['AV', ['83100', '83011', '83020']], ['BN', ['82100', '82011', '82020']], ['CE', ['81100', '81011', '81020']], ['NA', ['80100', '80121', '80131']], ['SA', ['84100', '84011', '84020']]],
    'Emilia-Romagna': [['BO', ['40100', '40121', '40131']], ['FC', ['47100', '47121', '47131']], ['FE', ['44100', '44121', '44131']], ['MO', ['41100', '41121', '41131']], ['PC', ['29100', '29121', '29131']], ['PR', ['43100', '43121', '43131']], ['RA', ['48100', '48121', '48131']], ['RE', ['42100', '42121', '42131']], ['RN', ['47900', '47921', '47931']]],
    'Friuli-Venezia Giulia': [['GO', ['34170', '34070', '34072']], ['PN', ['33170', '33170', '33170']], ['TS', ['34100', '34121', '34131']], ['UD', ['33100', '33170', '33010']]],
    'Lazio': [['FR', ['03100', '03121', '03131']], ['LT', ['04100', '04121', '04131']], ['RI', ['02100', '02121', '02131']], ['RM', ['00100', '00121', '00139']], ['VT', ['01100', '01121', '01131']]],
    'Liguria': [['GE', ['16100', '16121', '16131']], ['IM', ['18100', '18121', '18131']], ['SP', ['19100', '19121', '19131']], ['SV', ['17100', '17121', '17131']]],
    'Lombardia': [['BG', ['24100', '24121', '24131']], ['BS', ['25100', '25121', '25131']], ['CO', ['22100', '22121', '22131']], ['CR', ['26100', '26121', '26131']], ['LC', ['23900', '23921', '23931']], ['LO', ['26900', '26921', '26931']], ['MN', ['46100', '46121', '46131']], ['MI', ['20100', '20121', '20131']], ['MB', ['20900', '20921', '20931']], ['PV', ['27100', '27121', '27131']], ['SO', ['23100', '23121', '23131']], ['VA', ['21100', '21121', '21131']]],
    'Marche': [['AN', ['60100', '60121', '60131']], ['AP', ['63100', '63121', '63131']], ['FM', ['63900', '63921', '63931']], ['MC', ['62100', '62121', '62131']], ['PU', ['61100', '61121', '61131']]],
    'Molise': [['CB', ['86100', '86121', '86131']], ['IS', ['86170', '86171', '86179']]],
    'Piemonte': [['AL', ['15100', '15121', '15131']], ['AT', ['14100', '14121', '14131']], ['BI', ['13900', '13921', '13931']], ['CN', ['12100', '12121', '12131']], ['NO', ['28100', '28121', '28131']], ['TO', ['10100', '10121', '10131']], ['VB', ['28900', '28921', '28931']], ['VC', ['13100', '13121', '13131']]],
    'Puglia': [['BA', ['70100', '70121', '70131']], ['BT', ['76100', '76121', '76131']], ['BR', ['72100', '72121', '72131']], ['FG', ['71100', '71121', '71131']], ['LE', ['73100', '73121', '73131']], ['TA', ['74100', '74121', '74131']]],
    'Sardegna': [['CA', ['09100', '09121', '09131']], ['NU', ['08100', '08121', '08131']], ['OR', ['09170', '09171', '09179']], ['SS', ['07100', '07121', '07131']], ['SU', ['09010', '09011', '09019']]],
    'Sicilia': [['AG', ['92100', '92121', '92131']], ['CL', ['93100', '93121', '93131']], ['CT', ['95100', '95121', '95131']], ['EN', ['94100', '94121', '94131']], ['ME', ['98100', '98121', '98131']], ['PA', ['90100', '90121', '90131']], ['RG', ['97100', '97121', '97131']], ['SR', ['96100', '96121', '96131']], ['TP', ['91100', '91121', '91131']]],
    'Toscana': [['AR', ['52100', '52121', '52131']], ['FI', ['50100', '50121', '50131']], ['GR', ['58100', '58121', '58131']], ['LI', ['57100', '57121', '57131']], ['LU', ['55100', '55121', '55131']], ['MS', ['54100', '54121', '54131']], ['PI', ['56100', '56121', '56131']], ['PO', ['59100', '59121', '59131']], ['PT', ['51100', '51121', '51131']], ['SI', ['53100', '53121', '53131']]],
    'Trentino-Alto Adige': [['BZ', ['39100', '39121', '39131']], ['TN', ['38100', '38121', '38131']]],
    'Umbria': [['PG', ['06100', '06121', '06131']], ['TR', ['05100', '05121', '05131']]],
    'Valle d\'Aosta': [['AO', ['11100', '11121', '11131']]],
    'Veneto': [['BL', ['32100', '32121', '32131']], ['PD', ['35100', '35121', '35131']], ['RO', ['45100', '45121', '45131']], ['TV', ['31100', '31121', '31131']], ['VE', ['30100', '30121', '30131']], ['VI', ['36100', '36121', '36131']], ['VR', ['37100', '37121', '37131']]]
};

/**
 * Genera un codice IUN casuale nel formato specificato
 */
function generaIun() {
    const lettere = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    const numeri = '0123456789';
    const caratteri = lettere + numeri;

    const randomChar = () => caratteri[Math.floor(Math.random() * caratteri.length)];

    const parte1 = Array(4).fill().map(() => randomChar()).join('');
    const parte2 = Array(4).fill().map(() => randomChar()).join('');
    const parte3 = Array(4).fill().map(() => randomChar()).join('');
    const anno = ['202506', '202507', '202508'][Math.floor(Math.random() * 3)];
    const parte5 = Math.random() < 0.5 ? 'M' : 'F';
    const parte6 = Math.floor(Math.random() * 9) + 1;

    return `${parte1}-${parte2}-${parte3}-${anno}-${parte5}-${parte6}`;
}

/**
 * Genera 375 spedizioni per regione secondo le specifiche
 */
function generaSpedizioniPerRegione() {
    const spedizioni = [];

    // 182 spedizioni AR con attempt=0
    for (let i = 0; i < 182; i++) {
        spedizioni.push(['AR', 0]);
    }

    // 182 spedizioni 890 con attempt=0
    for (let i = 0; i < 182; i++) {
        spedizioni.push(['890', 0]);
    }

    // 2 spedizioni AR con attempt=1
    for (let i = 0; i < 2; i++) {
        spedizioni.push(['AR', 1]);
    }

    // 2 spedizioni 890 con attempt=1
    for (let i = 0; i < 2; i++) {
        spedizioni.push(['890', 1]);
    }

    // 7 spedizioni RS con attempt=0
    for (let i = 0; i < 7; i++) {
        spedizioni.push(['RS', 0]);
    }

    return spedizioni;
}

/**
 * Crea il modulo commessa JSON per una PA
 */
function creaModuloCommessa(senderPaId, valorePerRegione) {
    // Calcolo valori totali basati sul valore fisso per regione
    // 20 regioni * valore_per_regione per ogni prodotto
    const arValoreRegionale = valorePerRegione * 20;  // valore fisso per ogni regione * 20 regioni
    const arValoreTotale = arValoreRegionale + 700;  // + 700 per INT

    // Per 890: stesso valore per regione
    const p890ValoreRegionale = valorePerRegione * 20;  // valore fisso per ogni regione * 20 regioni
    const p890ValoreTotale = p890ValoreRegionale;

    const modulo = {
        "idEnte": senderPaId,
        "contractId": "5678abcftrs43d23el4",
        "periodo_riferimento": "9-2025",
        "last_update": "2025-01-01T00:00:00Z",
        "prodotti": [
            {
                "id": "AR",
                "nome": "AR",
                "valore_totale": arValoreTotale,
                "varianti": [
                    {
                        "codice": "NZ",
                        "nome": "NZ",
                        "valore_totale": arValoreRegionale,
                        "distribuzione": {
                            "regionale": []
                        }
                    },
                    {
                        "codice": "INT",
                        "nome": "INT",
                        "valore_totale": 700,
                        "distribuzione": null
                    }
                ]
            },
            {
                "id": "890",
                "nome": "890",
                "valore_totale": p890ValoreTotale,
                "varianti": [
                    {
                        "codice": "NZ",
                        "nome": "NZ",
                        "valore_totale": p890ValoreRegionale,
                        "distribuzione": {
                            "regionale": []
                        }
                    }
                ]
            },
            {
                "id": "digitale",
                "nome": "digitale",
                "valore_totale": 1200,
                "varianti": [
                    {
                        "codice": "PEC",
                        "nome": "PEC",
                        "valore_totale": 1200,
                        "distribuzione": null
                    }
                ]
            }
        ]
    };

    // Lista delle regioni
    const listaRegioni = [
        "Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna",
        "Friuli-Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche",
        "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana",
        "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto"
    ];

    // Aggiungi distribuzione regionale per AR NZ - ogni regione ha il valore fisso
    for (const regione of listaRegioni) {
        modulo.prodotti[0].varianti[0].distribuzione.regionale.push({
            "regione": regione,
            "valore": valorePerRegione,  // Valore fisso per ogni regione
            "province": null
        });
    }

    // Aggiungi distribuzione regionale per 890 NZ - ogni regione ha il valore fisso
    for (const regione of listaRegioni) {
        modulo.prodotti[1].varianti[0].distribuzione.regionale.push({
            "regione": regione,
            "valore": valorePerRegione,  // Valore fisso per ogni regione
            "province": null
        });
    }

    return modulo;
}

/**
 * Funzione principale
 */
async function main() {
    // Header del CSV
    const header = ['requestId', 'notificationSentAt', 'prepareRequestDate', 'productType', 'senderPaId', 'province', 'cap', 'attempt', 'iun'];

    // Lista di tutte le regioni
    const listaRegioni = Object.keys(regioni);

    // Parametri per la generazione
    const paPerFile = 4;  // Massimo 4 PA per file (30.000 spedizioni)
    const totalePa = 400;
    const totaleFile = Math.floor(totalePa / paPerFile);  // 100 file

    // Data di partenza per i timestamp
    const baseDateTime = new Date(2025, 8, 1, 1, 0, 0);  // Settembre = 8 (0-based)
    let timestampCounter = 0;
    let requestIdCounter = 1;

    const spedizioniDir = path.join(__dirname, 'spedizioni');
    const moduliCommessaDir = path.join(__dirname, 'moduliCommessa');

    // Genera i file CSV multipli
    for (let fileNum = 1; fileNum <= totaleFile; fileNum++) {
        const nomeFile = path.join(spedizioniDir, `spedizioni_pa_${fileNum.toString().padStart(3, '0')}.csv`);

        console.log(`Generando file ${fileNum}/${totaleFile}: ${nomeFile}`);

        let csvContent = header.join(';') + '\n';

        // Calcola range PA per questo file
        const paStart = (fileNum - 1) * paPerFile + 1;
        const paEnd = fileNum * paPerFile;

        // Per ogni PA in questo file (4 PA)
        for (let paNum = paStart; paNum <= paEnd; paNum++) {
            const senderPaId = `senderPaId${paNum}`;

            console.log(`  Generando dati per ${senderPaId}...`);

            // Genera modulo commessa per PA dalla 101esima in poi
            if (paNum >= 101) {
                // Determina il valore fisso per regione
                let valorePerRegione;
                if (paNum <= 200) {  // PA 101-200: primi 100
                    valorePerRegione = 250;
                } else {  // PA 201-400: ultimi 200
                    valorePerRegione = 10000;
                }

                const modulo = creaModuloCommessa(senderPaId, valorePerRegione);
                const nomeJson = path.join(moduliCommessaDir, `modulo_commessa_${senderPaId}.json`);

                fs.writeFileSync(nomeJson, JSON.stringify(modulo, null, 2), 'utf8');
                console.log(`  Creato modulo commessa: ${nomeJson} (valore per regione: ${valorePerRegione})`);
            }

            // Per ogni regione (20 regioni)
            for (const regione of listaRegioni) {
                const provinceRegione = regioni[regione];
                const spedizioniRegione = generaSpedizioniPerRegione();

                // Per ogni spedizione nella regione
                for (const [productType, attempt] of spedizioniRegione) {
                    // Scegli una provincia e CAP casuale dalla regione
                    const [provincia, capList] = provinceRegione[Math.floor(Math.random() * provinceRegione.length)];
                    const cap = capList[Math.floor(Math.random() * capList.length)];

                    // Genera timestamp incrementali
                    const notificationSentAt = new Date(baseDateTime.getTime() + timestampCounter * 1000).toISOString();
                    const prepareRequestDate = new Date(baseDateTime.getTime() + (timestampCounter + 1) * 1000).toISOString();
                    timestampCounter += 2;

                    // Genera dati per la riga
                    const requestId = `RequestId${requestIdCounter}`;
                    const iun = generaIun();

                    // Aggiungi la riga al CSV
                    const row = [
                        requestId,
                        notificationSentAt,
                        prepareRequestDate,
                        productType,
                        senderPaId,
                        provincia,
                        cap,
                        attempt,
                        iun
                    ];

                    csvContent += row.join(';') + '\n';
                    requestIdCounter++;
                }
            }
        }

        // Scrivi il file CSV
        fs.writeFileSync(nomeFile, csvContent, 'utf8');
        console.log(`  File '${nomeFile}' completato (30.000 spedizioni)\n`);
    }

    console.log('Generazione completata!');
    console.log(`Totale file CSV generati: ${totaleFile}`);
    console.log('Totale file JSON generati: 300 (PA 101-400)');
    console.log('  - 100 file JSON con valore 250 per regione (PA 101-200)');
    console.log('  - 200 file JSON con valore 10000 per regione (PA 201-400)');
    console.log('Spedizioni per file CSV: 30.000');
    console.log('PA per file CSV: 4');
    console.log(`Totale PA: ${totalePa}`);
    console.log(`Totale spedizioni: ${(totalePa * 7500).toLocaleString()}`);
}

// Esegui solo se il file viene eseguito direttamente
if (require.main === module) {
    main().catch(console.error);
}

module.exports = {
    generaIun,
    generaSpedizioniPerRegione,
    creaModuloCommessa,
    main
};