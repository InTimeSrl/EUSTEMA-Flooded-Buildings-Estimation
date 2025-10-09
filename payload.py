def _prepare_scenario_payload(payload):
    """
    Prepara il payload per il workflow, adattandolo alla struttura richiesta.
    """
    fixed_payload = {
        "elab_id": payload.get("elab_id", ""),
        "damage_threshold": payload.get("damage_threshold", 0.5),
        "min_overlap_percent": payload.get("min_overlap_percent", 90),
        "height_field_name": payload.get("height_field_name", None),
        "collapse_threshold_percent": payload.get("collapse_threshold_percent", 50.0),
        "dsm_pre": payload.get("dsm_pre", None),
        "dsm_post": payload.get("dsm_post", None),
        "buildings": payload.get("buildings", None),
        "dsm_difference": payload.get("dsm_difference", None),
        "results": payload.get("results", None)
    }
    return fixed_payload

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def _create_payload_from_config_tables():
    logging.debug(f"Creazione del payload a partire dalle tabelle di configurazione")
    payload = {
        "elab_id": f"e{datetime.now().strftime('%Y%m%d%H%M%S')}"
    }
    conf_dati_iniziale_df = dataiku.Dataset('configurazione_dati_iniziali').get_dataframe()
    for idx, row in conf_dati_iniziale_df.iterrows():
        section = row['sezione']
        variable_name = row['nome_variabile']
        variable_file_path = row['percorso_file']
        variable_data = {
            "percorso_variabile": variable_file_path,
            "nome_variabile": variable_name
        }
        if payload.get(section):
            payload[section].append(variable_data)
        else:
            payload[section] = [variable_data]
    conf_iniziale_df = dataiku.Dataset("configurazione_iniziale").get_dataframe()
    config_row = conf_iniziale_df.iloc[0].to_dict()
    config_row = {key: (int(value) if isinstance(value, np.int64) else value) for key, value in config_row.items()}
    payload.update(config_row)
    logging.debug(f"Creazione del payload completata")
    return payload

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def _create_payload():
    payload = None
    try:
        run_vars = dataiku.get_custom_variables()
        logging.debug(f"Recuperato variabili custom")
        scenario_run_id = run_vars.get('scenarioTriggerRunId')
        if scenario_run_id is not None:
            logging.info(f"Avvio recipe da scenario {scenario_run_id}")
            payload = json.loads(run_vars.get('scenarioTriggerParams'))
            payload = _prepare_scenario_payload(payload)
            if payload.get('elab_id') is None:
                payload["elab_id"] = f"e{scenario_run_id.replace('-', '')[:-3]}"
            else:
                payload["elab_id"] = f"e{payload['elab_id']}"
        else:
            logging.info(f"Avvio recipe da flow")
            payload = _create_payload_from_config_tables()
        logging.debug(f"{json.dumps(payload)}")
        return payload
    except Exception as ex:
        logging.error(f"{ex}")
        raise

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def _download_remote_to_tmp(remote_path: str, folder_obj, tmpdir: str):
    """
    Scarica risorsa remota da Dataiku Folder in una temp dir

    remote_path: percorso relativo nel folder (es. "/202509231300/output/merged_PRE.tif" o "202509231300/output/merged_PRE.tif")
    folder_obj: oggetto dataiku.Folder
    tmpdir: directory locale di destinazione
    Restituisce percorso locale al file principale (es. .tif o .shp) oppure None se non trovato.
    """
    try:
        tmpdir_path = tmpdir.name if hasattr(tmpdir, "name") else str(tmpdir)
    except Exception:
        tmpdir_path = str(tmpdir)
    print(f"tmpdir_path: {tmpdir_path}")
    if not remote_path:
        return None
    rp = str(remote_path).lstrip('/')
    try:
        file_list = folder_obj.list_paths_in_partition()
    except Exception:
        file_list = []

    # 1) match esatto (case-insensitive)
    for f in file_list:
        if f.lower() == rp.lower():
            local_p = os.path.join(tmpdir_path, os.path.basename(f))
            with folder_obj.get_download_stream(f) as stream, open(local_p, 'wb') as out:
                out.write(stream.read())
            return local_p

    # 2) se è uno shapefile: scarica tutti i files con lo stesso stem
    base_name = os.path.basename(rp)
    stem, ext = os.path.splitext(base_name)
    if ext.lower() == '.shp' and stem:
        # individua tutti i file che contengono lo stem (nella stessa dir o con stesso basename)
        matched = [f for f in file_list if os.path.basename(f).lower().startswith(stem.lower()) or f.lower().endswith('/' + stem.lower() + '.shp')]
        if not matched:
            # fallback: cerca per basename in tutto il folder
            matched = [f for f in file_list if os.path.basename(f).lower().startswith(stem.lower())]
        local_paths = []
        for f in matched:
            local_p = os.path.join(tmpdir_path, os.path.basename(f))
            try:
                with folder_obj.get_download_stream(f) as stream, open(local_p, 'wb') as out:
                    out.write(stream.read())
                local_paths.append(local_p)
            except Exception:
                continue
        # ritorna il .shp locale se presente, altrimenti il primo file scaricato
        for p in local_paths:
            if p.lower().endswith('.shp'):
                return p
        return local_paths[0] if local_paths else None

    # 3) match su filename alone (case-insensitive)
    for f in file_list:
        if os.path.basename(f).lower() == base_name.lower():
            local_p = os.path.join(tmpdir_path, os.path.basename(f))
            with folder_obj.get_download_stream(f) as stream, open(local_p, 'wb') as out:
                out.write(stream.read())
            return local_p

    # 4) match "endswith" (per sicurezza)
    for f in file_list:
        if f.lower().endswith(rp.lower()):
            local_p = os.path.join(tmpdir_path, os.path.basename(f))
            with folder_obj.get_download_stream(f) as stream, open(local_p, 'wb') as out:
                out.write(stream.read())
            return local_p

    return None

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def main():
    """
    Funzione principale di esecuzione per la rilevazione danni edifici
    """
    payload = _create_payload()
    if payload is None:
        logging.error("Errore nella creazione del payload")
        raise Exception("Errore nella creazione del payload")
    print(payload)

    ###########
    # Parametri di configurazione dal payload
    # Lista 'variabili' con oggetti {'percorso_variabile': ..., 'nome_variabile': ...}
    vars_list = payload.get("variabili") or []
    if isinstance(vars_list, dict):
        # Protezione nel caso payload variabili sia un singolo dict
        vars_list = [vars_list]

    # Prima prova a leggere DSM paths dalle variabili locali di progetto (impostate dalla recipe precedente)
    DSM_PRE_PATH = _get_project_local_var("DSM_PRE_PATH")
    DSM_POST_PATH = _get_project_local_var("DSM_POST_PATH")
    BUILDINGS_PATH = _get_project_local_var("BUILDINGS_PATH")
    OUTPUT_DIRECTORY = _get_project_local_var("OUTPUT_DIRECTORY")
    print(f"BUILDINGS_PATH variabili locali : {BUILDINGS_PATH}")

    # Se non trovate nelle variabili locali, esegui fallback sui valori forniti nel payload (se presenti)
    for v in vars_list:
        nome = v.get("nome_variabile")
        percorso = v.get("percorso_variabile")
        if not percorso:
            percorso = None
        # fallback: imposta solo se non già ottenuto da variabili locali
        if nome == "DSM_PRE_PATH" and not DSM_PRE_PATH:
            DSM_PRE_PATH = percorso
        elif nome == "DSM_POST_PATH" and not DSM_POST_PATH:
            DSM_POST_PATH = percorso
        elif nome == "BUILDINGS_PATH" and not BUILDINGS_PATH:
            BUILDINGS_PATH = percorso
            print(f"BUILDINGS_PATH variabili fallback : {BUILDINGS_PATH}")
        elif nome == "OUTPUT_DIRECTORY" and not OUTPUT_DIRECTORY:
            OUTPUT_DIRECTORY = percorso
   
    HEIGHT_FIELD_NAME = payload.get("height_field_name", "alt_tot")
    COLLAPSE_THRESHOLD_PERCENT = payload.get("collapse_threshold_percent", 50.0)
    try:
               
        tmp_local_pre = None
        tmp_local_post = None
        tmp_local_buildings = None
        temp_output_folder =None
        tmpdir =  tempfile.TemporaryDirectory()
        print(f"Cartella temporanea per download creata: {tmpdir}")
        folder_obj = minio_output
        folder_buildings = dataiku.Folder("minio_input")

        # tenta di scaricare i percorsi (se remoti), altrimenti ritorna None e useremo i percorsi originali
        tmp_local_pre = _download_remote_to_tmp(DSM_PRE_PATH, folder_obj, tmpdir) or DSM_PRE_PATH
        tmp_local_post = _download_remote_to_tmp(DSM_POST_PATH, folder_obj, tmpdir) or DSM_POST_PATH
        tmp_local_buildings = _download_remote_to_tmp(BUILDINGS_PATH, folder_buildings, tmpdir) or BUILDINGS_PATH
        temp_output_folder =os.path.join(tmpdir.name if hasattr(tmpdir, "name") else str(tmpdir), "output") or OUTPUT_DIRECTORY
        print("Percorsi locali usati per l'elaborazione:")
        print(f"  DSM_PRE:  {tmp_local_pre}")
        print(f"  DSM_POST: {tmp_local_post}")
        print(f"  BUILDINGS:{tmp_local_buildings}")
        print(f"  temp_output_folder:{temp_output_folder}")
       

        # Inizializza rilevatore (usa valori di default configurabili)
        detector = BuildingDamageDetector(
            height_field_name=HEIGHT_FIELD_NAME,
            collapse_threshold_percent=COLLAPSE_THRESHOLD_PERCENT
        )
        output_files = detector.process_complete_workflow(
                tmp_local_pre,
                tmp_local_post,
                tmp_local_buildings,
                temp_output_folder
            )

       


    ############
    # Parametri di configurazione
#     DSM_PRE_PATH = r"simul_data\INPUT\dsm000902_wor_32632.tif"
#     DSM_POST_PATH = r"simul_data\INPUT\dsm_crolli_simulati_32632.tif"
#     BUILDINGS_PATH = r"ancillary\edifici_TN_subset.shp"
#     OUTPUT_DIRECTORY = r"BDD_Analysis"

    # Nome del campo altezza nel vettoriale edifici (parametrizzabile)
#     HEIGHT_FIELD_NAME = "alt_tot"  # Cambiare se necessario

    # Soglia percentuale per classificazione crollo (parametrizzabile)
#     COLLAPSE_THRESHOLD_PERCENT = 50.0  # Cambiare se necessario (es. 30.0 per 30%)

    # Setup logging - cattura tutto l'output nel file BDD_execution.log
#     original_stdout, log_file = setup_logging(f"{OUTPUT_DIRECTORY}/BDD_execution.log")


#         # Inizializza rilevatore (usa valori di default configurabili)
#         detector = BuildingDamageDetector(
#             height_field_name=HEIGHT_FIELD_NAME,
#             collapse_threshold_percent=COLLAPSE_THRESHOLD_PERCENT
#         )

#         # Esegue flusso di lavoro completo
#         output_files = detector.process_complete_workflow(
#             DSM_PRE_PATH,
#             DSM_POST_PATH,
#             BUILDINGS_PATH,
#             OUTPUT_DIRECTORY
#         )

        # Ripristina stdout originale per messaggio finale
#         sys.stdout = original_stdout

        if output_files is None:
            print(f"❌ ESECUZIONE FALLITA - Exit Code: 1")
#             print(f"📄 Log completo: {log_file}")
            return 1  # Exit code per indicare fallimento
        try:
            output_folder = bdd_results  # Dataiku Folder oggetto destinazione
            output_folder_path = output_folder.get_path()

            # Determina codice di partition (es. "202509231300") a partire dal percorso originale
            base_code = None
            vars_list_payload = payload.get("variabili") if payload else []
            if isinstance(vars_list_payload, dict):
                vars_list_payload = [vars_list_payload]
            for v in vars_list_payload:
                if v.get("nome_variabile") == "INPUT_FOLDER_PRE":
                    pv = v.get("percorso_variabile") or ""
                    parts = pv.strip("/").split("/")
                    if parts and parts[0]:
                        base_code = parts[0]
                        break

            # fallback: prova da OUTPUT_DIRECTORY o usa timestamp
            if not base_code:
                if OUTPUT_DIRECTORY:
                    parts = str(OUTPUT_DIRECTORY).strip("/").split("/")
                    base_code = parts[0] if parts and parts[0] else None
            if not base_code:
                base_code = datetime.now().strftime("%Y%m%d%H%M%S")

            # usa il nome finale di OUTPUT_DIRECTORY come sottocartella, altrimenti 'output'
            output_sub = "output"
            if OUTPUT_DIRECTORY:
                output_sub = os.path.basename(str(OUTPUT_DIRECTORY).rstrip("/")) or "output"

            partition_output_dir = os.path.join(output_folder_path, base_code)
            os.makedirs(partition_output_dir, exist_ok=True)

            # copia file e directory dal temp_output_folder nella cartella di destinazione
            if os.path.isdir(temp_output_folder):
                for item_name in os.listdir(temp_output_folder):
                    src_path = os.path.join(temp_output_folder, item_name)
                    dest_path = os.path.join(partition_output_dir, item_name)

                    if os.path.isfile(src_path):
                        shutil.copy2(src_path, dest_path)
                    elif os.path.isdir(src_path):
                        shutil.copytree(src_path, dest_path, dirs_exist_ok=True)

            print(f"✅ Dati copiati da temp_output_folder → {partition_output_dir}")

        except Exception as ex:
            print(f"⚠️ Errore durante copia output su OUTPUT_DIRECTORY: {ex}")

        print(f"✅ RILEVAZIONE DANNI EDIFICI COMPLETATA - Exit Code: 0")
        print(f"📁 Output files: {len(output_files)} generati")
#         print(f"📄 Log dettagliato: {log_file}")

        return 0

    except Exception as e:
        # Ripristina stdout per errori
#         sys.stdout = original_stdout
        print(f"❌ ERRORE CRITICO - Exit Code: 1")
        print(f"🔥 Errore: {str(e)}")
#         print(f"📄 Log completo: {log_file}")

        # Log anche l'errore nel file
        logging.error(f"ERRORE CRITICO: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")

        return 1