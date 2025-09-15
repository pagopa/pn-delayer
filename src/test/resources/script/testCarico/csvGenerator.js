const fs = require('fs');
const path = require('path');

// Italian regions with provinces and postal codes
const regioni = {
    'Abruzzo': [
        ['AQ', ['67100', '67039', '67051']],
        ['CH', ['66100', '66034', '66041']],
        ['PE', ['65122', '65121', '65129']],
        ['TE', ['64100', '64011', '64020']]
    ],
    'Basilicata': [
        ['MT', ['75100', '75011', '75019']],
        ['PZ', ['85100', '85020', '85030']]
    ],
    'Calabria': [
        ['CS', ['87100', '87011', '87020']],
        ['CZ', ['88100', '88021', '88020']],
        ['KR', ['88900', '88811', '88812']],
        ['RC', ['89121', '89011', '89020']],
        ['VV', ['89900', '89812', '89813']]
    ],
    'Campania': [
        ['AV', ['83100', '83011', '83020']],
        ['BN', ['82100', '82011', '82020']],
        ['CE', ['81100', '81011', '81020']],
        ['NA', ['80124', '80121', '80131']],
        ['SA', ['84121', '84011', '84020']]
    ],
    'Emilia-Romagna': [
        ['BO', ['40122', '40121', '40131']],
        ['FC', ['47122', '47121', '47010']],
        ['FE', ['44122', '44121', '44123']],
        ['MO', ['41122', '41121', '41123']],
        ['PC', ['29122', '29121', '29010']],
        ['PR', ['43122', '43121', '43123']],
        ['RA', ['48122', '48121', '48123']],
        ['RE', ['42122', '42121', '42123']],
        ['RN', ['47922', '47921', '47923']]
    ],
    'Friuli-Venezia Giulia': [
        ['GO', ['34170', '34070', '34072']],
        ['PN', ['33170', '33170', '33170']],
        ['TS', ['34122', '34121', '34131']],
        ['UD', ['33100', '33011', '33010']]
    ],
    'Lazio': [
        ['FR', ['03100', '03010', '03011']],
        ['LT', ['04100', '04010', '04011']],
        ['RI', ['02100', '02010', '02011']],
        ['RM', ['00118', '00121', '00139']],
        ['VT', ['01100', '01010', '01011']]
    ],
    'Liguria': [
        ['GE', ['16122', '16121', '16131']],
        ['IM', ['18100', '18010', '18011']],
        ['SP', ['19122', '19121', '19131']],
        ['SV', ['17100', '17010', '17011']]
    ],
    'Lombardia': [
        ['BG', ['24122', '24121', '24123']],
        ['BS', ['25122', '25121', '25131']],
        ['CO', ['22100', '22010', '22011']],
        ['CR', ['26100', '26010', '26011']],
        ['LC', ['23900', '23801', '23802']],
        ['LO', ['26900', '26811', '26812']],
        ['MN', ['46100', '46010', '46011']],
        ['MI', ['20122', '20121', '20131']],
        ['MB', ['20900', '20811', '20812']],
        ['PV', ['27100', '27010', '27011']],
        ['SO', ['23100', '23009', '23010']],
        ['VA', ['21100', '21009', '21010']]
    ],
    'Marche': [
        ['AN', ['60122', '60121', '60131']],
        ['AP', ['63100', '63061', '63062']],
        ['FM', ['63900', '63811', '63812']],
        ['MC', ['62100', '62010', '62011']],
        ['PU', ['61122', '61121', '61010']]
    ],
    'Molise': [
        ['CB', ['86100', '86010', '86011']],
        ['IS', ['86170', '86070', '86071']]
    ],
    'Piemonte': [
        ['AL', ['15122', '15121', '15010']],
        ['AT', ['14100', '14010', '14011']],
        ['BI', ['13900', '13811', '13812']],
        ['CN', ['12100', '12010', '12011']],
        ['NO', ['28100', '28010', '28011']],
        ['TO', ['10122', '10121', '10131']],
        ['VB', ['28922', '28921', '28923']],
        ['VC', ['13100', '13010', '13011']]
    ],
    'Puglia': [
        ['BA', ['70122', '70121', '70131']],
        ['BT', ['76123', '76121', '76125']],
        ['BR', ['72100', '72012', '72013']],
        ['FG', ['71122', '71121', '71010']],
        ['LE', ['73100', '73010', '73011']],
        ['TA', ['74122', '74121', '74123']]
    ],
    'Sardegna': [
        ['CA', ['09122', '09121', '09131']],
        ['NU', ['08100', '08010', '08011']],
        ['OR', ['09170', '09070', '09071']],
        ['SS', ['07100', '07010', '07011']],
        ['SU', ['09010', '09011', '09019']]
    ],
    'Sicilia': [
        ['AG', ['92100', '92010', '92011']],
        ['CL', ['93100', '93010', '93011']],
        ['CT', ['95122', '95121', '95131']],
        ['EN', ['94100', '94010', '94011']],
        ['ME', ['98122', '98121', '98131']],
        ['PA', ['90123', '90121', '90131']],
        ['RG', ['97100', '97010', '97011']],
        ['SR', ['96100', '96010', '96011']],
        ['TP', ['91100', '91010', '91011']]
    ],
    'Toscana': [
        ['AR', ['52100', '52010', '52011']],
        ['FI', ['50122', '50121', '50131']],
        ['GR', ['58100', '58010', '58011']],
        ['LI', ['57122', '57121', '57123']],
        ['LU', ['55100', '55010', '55011']],
        ['MS', ['54100', '54010', '54011']],
        ['PI', ['56122', '56121', '56123']],
        ['PO', ['59100', '59011', '59013']],
        ['PT', ['51100', '51010', '51011']],
        ['SI', ['53100', '53011', '53012']]
    ],
    'Trentino-Alto Adige': [
        ['BZ', ['39100', '39010', '39011']],
        ['TN', ['38122', '38121', '38123']]
    ],
    'Umbria': [
        ['PG', ['06122', '06121', '06131']],
        ['TR', ['05100', '05010', '05011']]
    ],
    'Valle d\'Aosta': [
        ['AO', ['11100', '11010', '11011']]
    ],
    'Veneto': [
        ['BL', ['32100', '32010', '32012']],
        ['PD', ['35122', '35121', '35131']],
        ['RO', ['45100', '45010', '45011']],
        ['TV', ['31100', '31010', '31011']],
        ['VE', ['30122', '30121', '30123']],
        ['VI', ['36100', '36010', '36011']],
        ['VR', ['37122', '37121', '37131']]
    ]
};

/**
 * Generate a random IUN code in the specified format
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
 * Generate 375 shipments per region according to specifications
 */
function generaSpedizioniPerRegione() {
    const spedizioni = [];

    // 182 AR shipments with attempt=0
    for (let i = 0; i < 182; i++) {
        spedizioni.push(['AR', 0]);
    }

    // 182 890 shipments with attempt=0
    for (let i = 0; i < 182; i++) {
        spedizioni.push(['890', 0]);
    }

    // 2 AR shipments with attempt=1
    for (let i = 0; i < 2; i++) {
        spedizioni.push(['AR', 1]);
    }

    // 2 890 shipments with attempt=1
    for (let i = 0; i < 2; i++) {
        spedizioni.push(['890', 1]);
    }

    // 7 RS shipments with attempt=0
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

    const listaRegioni = [
        "Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna",
        "Friuli-Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche",
        "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana",
        "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto"
    ];

    // Add regional distribution for AR NZ - each region has a fixed value
    for (const regione of listaRegioni) {
        modulo.prodotti[0].varianti[0].distribuzione.regionale.push({
            "regione": regione,
            "valore": valorePerRegione,
            "province": null
        });
    }

    // Add regional distribution for 890 NZ - each region has a fixed value
    for (const regione of listaRegioni) {
        modulo.prodotti[1].varianti[0].distribuzione.regionale.push({
            "regione": regione,
            "valore": valorePerRegione,  // Valore fisso per ogni regione
            "province": null
        });
    }

    return modulo;
}

async function main() {
    // CSV header
    const header = ['requestId', 'notificationSentAt', 'prepareRequestDate', 'productType', 'senderPaId', 'province', 'cap', 'attempt', 'iun'];

    const listaRegioni = Object.keys(regioni);

    // Generation parameters
    const paPerFile = 4;  // Massimo 4 PA per file (30.000 spedizioni)
    const totalePa = 400;
    const totaleFile = Math.floor(totalePa / paPerFile);  // 100 file

    // Starting date for timestamps
    const baseDateTime = new Date(2025, 8, 1, 1, 0, 0);  // Settembre = 8 (0-based)
    let timestampCounter = 0;
    let requestIdCounter = 1;

    const spedizioniDir = path.join(__dirname, 'spedizioni');
    const moduliCommessaDir = path.join(__dirname, 'moduliCommessa');

    // Generate multiple CSV files
    for (let fileNum = 1; fileNum <= totaleFile; fileNum++) {
        const nomeFile = path.join(spedizioniDir, `spedizioni_pa_${fileNum.toString().padStart(3, '0')}.csv`);

        console.log(`Generando file ${fileNum}/${totaleFile}: ${nomeFile}`);

        let csvContent = header.join(';') + '\n';

        // Calculate PA range for this file
        const paStart = (fileNum - 1) * paPerFile + 1;
        const paEnd = fileNum * paPerFile;

        // For each PA in this file (4 PAs)
        for (let paNum = paStart; paNum <= paEnd; paNum++) {
            const senderPaId = `loadTest_PaId${paNum}`;

            console.log(`  Generando dati per ${senderPaId}...`);

            // Generate from 101st onwards
            if (paNum >= 101) {
                let valorePerRegione;
                if (paNum <= 200) {  // PA 101-200: first 100
                    valorePerRegione = 250;
                } else {  // PA 201-400: last 200
                    valorePerRegione = 10000;
                }

                const modulo = creaModuloCommessa(senderPaId, valorePerRegione);
                const nomeJson = path.join(moduliCommessaDir, `modulo_commessa_${senderPaId}.json`);

                fs.writeFileSync(nomeJson, JSON.stringify(modulo, null, 2), 'utf8');
                console.log(`  Creato modulo commessa: ${nomeJson} (valore per regione: ${valorePerRegione})`);
            }

            for (const regione of listaRegioni) {
                const provinceRegione = regioni[regione];
                const spedizioniRegione = generaSpedizioniPerRegione();

                for (const [productType, attempt] of spedizioniRegione) {
                    const [provincia, capList] = provinceRegione[Math.floor(Math.random() * provinceRegione.length)];
                    const cap = capList[Math.floor(Math.random() * capList.length)];

                    const notificationSentAt = new Date(baseDateTime.getTime() + timestampCounter * 1000).toISOString();
                    const prepareRequestDate = new Date(baseDateTime.getTime() + (timestampCounter + 1) * 1000).toISOString();
                    timestampCounter += 2;

                    const requestId = `RequestId${requestIdCounter}`;
                    const iun = generaIun();

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

module.exports = {
    generaIun,
    generaSpedizioniPerRegione,
    creaModuloCommessa,
    main
};