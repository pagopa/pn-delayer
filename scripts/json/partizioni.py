
import json
import ast

def generate_unique_delivery_driver_province_list_json(output_filename="delivery_driver_province_list.json"):
    """
    Genera una lista di stringhe univoche composte da deliveryDriverId##provincia
    e la scrive in un file JSON.

    Args:
        output_filename (str, optional): Il nome del file JSON di output.
                                         Defaults to "delivery_driver_province_list.json".
    """
    delivery_driver_ids = range(1, 7)  # Numeri da 1 a 6
    province_list_it = get_italian_provinces()
    unique_list = []

    for driver_id in delivery_driver_ids:
        for provincia in province_list_it:
            unique_list.append(f"{driver_id}##{provincia}")

    try:
        with open(output_filename, 'w', encoding='utf-8') as outfile:
            json.dump(unique_list, outfile, indent=4)  # Usa indent per una migliore leggibilità
        print(f"La lista è stata scritta nel file JSON: {output_filename}")
    except Exception as e:
        print(f"Si è verificato un errore durante la scrittura nel file JSON: {e}")

def get_italian_provinces():
    province_sigle_list_str_107_sigle = "['AG', 'AL', 'AN', 'AO', 'AR', 'AP', 'AT', 'AV', 'BA', 'BT', 'BL', 'BN', 'BG', 'BI', 'BO', 'BZ', 'BS', 'BR', 'CA', 'CL', 'CB', 'CE', 'CT', 'CZ', 'CH', 'CO', 'CS', 'CR', 'KR', 'CN', 'EN', 'FM', 'FE', 'FI', 'FG', 'FC', 'FR', 'GE', 'GO', 'GR', 'IM', 'IS', 'AQ', 'SP', 'LT', 'LE', 'LC', 'LI', 'LO', 'LU', 'MC', 'MN', 'MS', 'MT', 'ME', 'MI', 'MO', 'MB', 'NA', 'NO', 'NU', 'OR', 'PD', 'PA', 'PR', 'PV', 'PG', 'PU', 'PE', 'PC', 'PI', 'PT', 'PN', 'PZ', 'PO', 'RG', 'RA', 'RC', 'RE', 'RI', 'RN', 'RM', 'RO', 'SA', 'SS', 'SV', 'SI', 'SR', 'SO', 'TA', 'TE', 'TR', 'TO', 'TP', 'TN', 'TV', 'TS', 'UD', 'VA', 'VE', 'VB', 'VC', 'VR', 'VV', 'VI', 'VT']"
    return ast.literal_eval(province_sigle_list_str_107_sigle)

if __name__ == "__main__":
    generate_unique_delivery_driver_province_list_json()