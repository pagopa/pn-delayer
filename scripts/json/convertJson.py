import json
from datetime import datetime, timezone

def map_object(input_object):
    """
    Mappa un singolo oggetto JSON secondo le regole specificate.

    Args:
        input_object (dict): L'oggetto JSON di input.

    Returns:
        dict: L'oggetto JSON di output mappato.
    """
    output_object = {}

    # Estrai i dati dall'oggetto JSON di input
    pk = input_object.get("pk")
    createdAt = input_object.get("createdAt")
    productType = input_object.get("productType")
    requestId = input_object.get("requestId")
    iun = input_object.get("iun")
    deliveryDriverId = input_object.get("deliveryDriverId")
    tenderId = input_object.get("tenderId")
    senderPaId = input_object.get("senderPaId")
    province = input_object.get("province")
    cap = input_object.get("cap")

    # Esegui la mappatura
    output_object["pk"] = f"{deliveryDriverId}##{province}"
    now_utc = datetime.now(timezone.utc)
    output_object["createdAt"] = createdAt
    output_object["requestId"] = requestId
    output_object["productType"] = productType
    output_object["cap"] = cap
    output_object["province"] = province
    output_object["senderPaId"] = senderPaId
    output_object["iun"] = iun
    output_object["deliveryDriverId"] = deliveryDriverId
    output_object["tenderId"] = tenderId

    return output_object

def convert_list_of_objects(input_file_path, output_file_path):
    """
    Legge un file JSON contenente una lista di oggetti,
    converte ogni oggetto e scrive la lista degli oggetti convertiti in un nuovo file JSON.

    Args:
        input_file_path (str): Il percorso del file JSON di input.
        output_file_path (str): Il percorso del file JSON di output.
    """
    try:
        with open(input_file_path, 'r') as f:
            data = json.load(f)
            if not isinstance(data, list):
                print("Errore: Il file JSON di input non contiene una lista.")
                return

            output_list = [map_object(item) for item in data]

        with open(output_file_path, 'w') as outfile:
            json.dump(output_list, outfile, indent=4)

        print(f"Conversione completata. I risultati sono stati scritti in: {output_file_path}")

    except FileNotFoundError:
        print(f"Errore: Il file di input '{input_file_path}' non è stato trovato.")
    except json.JSONDecodeError:
        print(f"Errore: Impossibile decodificare il file JSON '{input_file_path}'.")
    except Exception as e:
        print(f"Si è verificato un errore: {e}")

# Esempio di utilizzo:
if __name__ == "__main__":
    input_file = "PaperRequestDeliveryRouting.json"  # Sostituisci con il percorso del tuo file di input
    output_file = "PaperDeliveryHighPriority.json" # Sostituisci con il percorso del tuo file di output
    convert_list_of_objects(input_file, output_file)