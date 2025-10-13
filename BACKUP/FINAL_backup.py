# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Integrazione wd_estimation.py con Dataiku DSS
# 
# Implementazione dello script di analisi della sommersione degli edifici (`wd_estimation.py`) in un recipe Python di Dataiku DSS.
# 
# ## Obiettivo
# Calcolare la percentuale di sommersione degli edifici durante eventi alluvionali analizzando la profondità dell'acqua nei pixel esterni al perimetro di ciascun edificio.
# 
# ## 🎭 **Modalità di Esecuzione**
# 
# Il sistema supporta due modalità operative con rilevamento automatico:
# 
# ### **👤 ESECUZIONE MANUALE**
# - Operatore modifica dataset `configurazione_parametri`
# - Esecuzione manuale del recipe
# - Sistema legge parametri dai dataset Dataiku
# 
# ### **🤖 ESECUZIONE AUTOMATICA (Scenario Avanzato)**
# - Sistema attivato automaticamente (timer, trigger, API)
# - Parametri passati tramite **JSON Scenario Variables**
# - **Integrazione automatica** con dataset Dataiku per parametri mancanti
# 
# ## ⚖️ **Sistema di Priorità dei Parametri**
# 
# Il sistema implementa una gerarchia di priorità dei parametri:
# 
# 1. **🥇 Scenario JSON** (Priorità MASSIMA)
#    - Parametri espliciti nel JSON scenario sovrascrivono tutto
#    
# 2. **🥈 Dataset Dataiku** (Priorità MEDIA) 
#    - Integrazione automatica per parametri mancanti nel JSON
#    - `configurazione_parametri` e `configurazione_dati`
#    
# 3. **🥉 Valori Default** (Priorità MINIMA)
#    - Fallback finale per parametri non specificati
# 
# **💡 ESEMPIO PRATICO**: 
# - JSON fornisce solo `files` → Sistema integra automaticamente `HEIGHT_FIELD`, `EPSG`, etc. dai dataset Dataiku
# - JSON fornisce parametri completi → Usa solo quelli specificati, ignora dataset
# 
# ### **📋 Esempi JSON Scenario Variables**
# 
# #### **🟢 Scenario MINIMO (Auto-integrazione):**
# ```json
# {
#   "files": {
#     "vettoriale": "edifici.shp", 
#     "raster": "flood_depth.tif"
#   }
# }
# ```
# *→ Sistema integra automaticamente tutti i parametri mancanti dai dataset Dataiku*
# 
# #### **🔧 Scenario COMPLETO (Override totale):**
# ```json
# {
#   "elab_id": "flood_analysis_20241008",
#   "event_name": "Alluvione Tevere Ottobre 2024",
#   "files": {
#     "vettoriale": "edifici_roma.shp",
#     "raster": "flood_depth_tevere.tif"
#   },
#   "HEIGHT_FIELD": "h_uvl",
#   "BUFFER_DISTANCE": 2.5,
#   "TARGET_EPSG": "32632",
#   "REPROJECTION_OPTION": 3,
#   "min_valid_height": 0.5,
#   "enable_logging": true,
#   "create_report": true,
#   "create_shapefile": false
# }
# ```
# *→ Usa solo parametri JSON, ignora dataset Dataiku*
# 
# #### **🎨 Scenario NAMING PERSONALIZZATO (Controllo completo output):**
# ```json
# {
#   "files": {
#     "vettoriale": "edifici_tevere.shp",
#     "raster": "flood_depth_tevere.tif"
#   },
#   "event_name": "Alluvione_Tevere_2024",
#   "output_naming": {
#     "dataset_name": "risultati_alluvione_tevere",
#     "folder_name": "output_tevere_analisi",
#     "file_prefix": "tevere_flood_",
#     "file_suffix": "final",
#     "include_timestamp": true
#   }
# }
# ```
# *→ Personalizza completamente nomi di dataset, folder e file output*
# 
# **🚀 Il sistema si adatta automaticamente alla modalità di esecuzione**
# 
# ## Input Dataiku
# - **Dataset**: `configurazione_parametri` - Contiene i parametri di configurazione (HEIGHT_FIELD, REPROJECTION_OPTION, TARGET_EPSG, BUFFER_DISTANCE)
# - **Dataset**: `configurazione_dati` - Contiene la selezione dei file di input (tipo_file, nome_file) per file vettoriali e raster specifici
# - **Folder**: `minio_input` - Folder Minio con file geospaziali organizzati in sottocartelle
# 
# ## Formati Supportati
# ### 📂 **Formati Vettoriali**:
# - **Shapefile** (`.shp`) - formato standard ESRI con file accessori
# - **GeoJSON** (`.geojson`, `.json`) - formato JSON geografico
# - **GeoPackage** (`.gpkg`) - formato moderno OGC 
# - **GeoParquet** (`.parquet`, `.geoparquet`) - formato colonnare ottimizzato per performance
# - **KML** (`.kml`) - formato Google Earth
# - **GML** (`.gml`) - Geography Markup Language
# 
# ### 🗺️ **Formati Raster**:
# - **GeoTIFF** (`.tif`, `.tiff`) - formato standard georeferenziato
# - **ERDAS Imagine** (`.img`) - formato imaging professionale
# - **JPEG2000** (`.jp2`) - compressione avanzata con georiferimento
# - **Immagini standard** (`.png`, `.jpg`, `.jpeg`, `.bmp`, `.gif`) - con world file
# 
# ## 🎛️ **Controlli di Output**
# 
# Il sistema supporta controlli granulari per personalizzare l'output:
# 
# - **`enable_logging`**: Attiva/disattiva logging dettagliato (default: `true`)
# - **`create_report`**: Genera report statistico HTML (default: `true`) 
# - **`create_shapefile`**: Salva risultati come shapefile (default: `true`)
# 
# **Esempio controllo output**:
# ```json
# {
#   "files": { "vettoriale": "edifici.shp", "raster": "flood.tif" },
#   "enable_logging": false,
#   "create_report": true,  
#   "create_shapefile": false
# }
# ```
# 
# ## Output Dataiku  
# - **Dataset**: `output_inondazioni` - DataFrame con risultati dell'analisi di sommersione
# - **Folder**: `output_inondazioni` - Folder con shapefile risultanti, report statistici e file di log (condizionali)
# 
# ## 🧪 **Framework di Testing**
# 
# Il notebook include un **sistema di test isolato** (celle finali) per validare scenari JSON senza interferire con il workflow principale:
# 
# - **Test BASE**: JSON minimale con integrazione Dataiku
# - **Test COMPLETO**: JSON con tutti i parametri
# - **Test ERROR**: Validazione gestione errori
# - **Test CUSTOM**: Scenario personalizzabile
# - **Test NAMING**: Naming output personalizzabile
# 
# ## Metodologia
# L'analisi utilizza la tecnica del **campionamento esterno dei pixel** per determinare la profondità dell'acqua attorno agli edifici, calcolando statistiche di sommersione basate sul rapporto tra profondità media dell'acqua e altezza dell'edificio.
# 
# ### ⚠️ **Nota Tecnica - Buffer Distance**
# Il parametro `BUFFER_DISTANCE` definisce la distanza (in metri) del buffer attorno agli edifici per il campionamento dei pixel:
# - **Automatico** (`auto`): usa la risoluzione spaziale del raster (consigliato)
# - **Manuale**: valore in metri - **IMPORTANTE**: deve essere ≥ 50% della risoluzione pixel per risultati affidabili
# - **Esempio**: con risoluzione 1m, buffer < 0.5m può produrre campioni insufficienti

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## 1. Setup e Configurazione
# 
# Import delle librerie necessarie e configurazione dei parametri dal dataset Dataiku.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
# Import librerie di base
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Import librerie geospaziali
import geopandas as gpd
import rasterio
import rasterio.mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
from shapely.geometry import mapping
from shapely.ops import unary_union
import fiona

# Import librerie di utilità
import os
import sys
import tempfile
import logging
from datetime import datetime
import pytz
import shutil
import warnings
from io import StringIO
import json

# Configurazione per sopprimere warning di librerie geospaziali
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message=".*Input shapes do not overlap raster.*")
warnings.filterwarnings("ignore", message=".*invalid value encountered.*")

# Classe per catturare le stampe SOLO nel log (senza output a schermo)
class LogCapture:
    def __init__(self):
        self.log_buffer = StringIO()
        self.original_stdout = sys.stdout
        
    def write(self, text):
        # Scrivi SOLO nel buffer di log - niente a schermo
        self.log_buffer.write(text)
        
    def flush(self):
        self.log_buffer.flush()
        
    def get_log_content(self):
        return self.log_buffer.getvalue()
        
    def clear_log(self):
        self.log_buffer.close()
        self.log_buffer = StringIO()
        
    def close(self):
        self.log_buffer.close()

# SISTEMA ERROR HANDLING ROBUSTO per FLOOD ANALYSIS
class FloodAnalysisError(Exception):
    """Eccezione personalizzata per errori di flood analysis"""
    pass

class ErrorHandler:
    """
    Gestore errori centralizzato per analisi sommersione
    Traccia, categorizza e gestisce tutti gli errori del workflow
    """
    
    def __init__(self, config):
        self.config = config
        self.errors = []
        self.warnings = []
        self.stats = {
            'file_errors': 0,
            'processing_errors': 0, 
            'validation_errors': 0,
            'geometry_errors': 0,
            'data_errors': 0
        }
    
    def handle_file_error(self, operation: str, filename: str, error: Exception):
        """Gestisce errori di file I/O"""
        msg = f"File {operation} failed for {filename}: {str(error)}"
        self.errors.append(('FILE_ERROR', msg, error))
        self.stats['file_errors'] += 1
        print(f"❌ FILE ERROR: {msg}")
        return False
    
    def handle_processing_error(self, building_id: str, operation: str, error: Exception):
        """Gestisce errori di processing degli edifici"""
        msg = f"Processing {operation} failed for building {building_id}: {str(error)}"
        self.errors.append(('PROCESSING_ERROR', msg, error))
        self.stats['processing_errors'] += 1
        # Non stampare - troppo verboso per migliaia di edifici
        return False
    
    def handle_validation_error(self, validation_type: str, details: str):
        """Gestisce errori di validazione"""
        msg = f"Validation failed: {validation_type} - {details}"
        self.errors.append(('VALIDATION_ERROR', msg, None))
        self.stats['validation_errors'] += 1
        print(f"⚠️ VALIDATION ERROR: {msg}")
        return False
    
    def handle_geometry_error(self, building_id: str, operation: str, error: Exception):
        """Gestisce errori geometrici"""
        msg = f"Geometry {operation} failed for building {building_id}: {str(error)}"
        self.errors.append(('GEOMETRY_ERROR', msg, error))
        self.stats['geometry_errors'] += 1
        return False
    
    def add_warning(self, warning_type: str, message: str):
        """Aggiunge warning non bloccante"""
        self.warnings.append((warning_type, message))
        print(f"⚠️ WARNING: {message}")
    
    def get_error_summary(self):
        """Ritorna summary degli errori per report"""
        total_errors = len(self.errors)
        summary = {
            'total_errors': total_errors,
            'total_warnings': len(self.warnings),
            'stats': self.stats.copy(),
            'has_critical_errors': any(error[0] in ['FILE_ERROR', 'VALIDATION_ERROR'] for error in self.errors)
        }
        return summary
    
    def should_continue_processing(self):
        """Determina se continuare processing nonostante errori"""
        # Stop solo per errori critici (file, validazione)
        critical_errors = [e for e in self.errors if e[0] in ['FILE_ERROR', 'VALIDATION_ERROR']]
        return len(critical_errors) == 0
    
    def print_final_report(self):
        """Stampa report finale errori"""
        print(f"\n=== ERROR HANDLING REPORT ===")
        print(f"Total errors: {len(self.errors)}")
        print(f"Total warnings: {len(self.warnings)}")
        for error_type, count in self.stats.items():
            if count > 0:
                print(f"  {error_type}: {count}")
        
        if self.warnings:
            print(f"\nTop 5 warnings:")
            for i, (warn_type, msg) in enumerate(self.warnings[:5]):
                print(f"  {i+1}. [{warn_type}] {msg}")

# ATTIVA LOGGING GLOBALE PER CATTURARE TUTTE LE STAMPE
# Ripristina stdout se già attivo, poi ricrea il sistema di logging
if 'log_capture' in globals():
    sys.stdout = log_capture.original_stdout
    log_capture.close()

log_capture = LogCapture()
sys.stdout = log_capture

print("✅ Tutte le librerie importate con successo")
print("📝 Sistema di logging attivato - OUTPUT NASCOSTO")

# Ripristina stdout per mostrare solo questo messaggio di conferma
sys.stdout = log_capture.original_stdout
print("🔇 MODALITÀ SILENZIOSA ATTIVATA - Output nascosto (visibile solo nel log finale)")
sys.stdout = log_capture

# Funzioni payload integrate per configurazione avanzata
def _create_payload():
    """Crea payload unificato per analisi inondazioni"""
    payload = {
        "elab_id": f"flood_{datetime.now(pytz.timezone('Europe/Rome')).strftime('%Y%m%d_%H%M%S')}"
    }
    
    # Leggi parametri da tabelle esistenti
    try:
        conf_params = dataiku.Dataset("configurazione_parametri").get_dataframe()
        for _, row in conf_params.iterrows():
            payload[row['variabile']] = row['valore']
    except:
        pass
    
    # Leggi file da configurazione dati
    try:
        conf_data = dataiku.Dataset("configurazione_dati").get_dataframe()
        payload['files'] = {}
        for _, row in conf_data.iterrows():
            payload['files'][row['tipo_file']] = row['nome_file']
    except:
        payload['files'] = {}
    
    return payload

def _download_remote_to_tmp(file_path: str, folder_obj, tmpdir: str):
    """
    Scarica file da Dataiku Folder in directory temporanea
    Versione adattata per flood analysis con file semplici
    
    file_path: percorso del file nel folder
    folder_obj: oggetto dataiku.Folder
    tmpdir: directory locale di destinazione
    Restituisce percorso locale al file scaricato oppure None se non trovato.
    """
    if not file_path:
        return None
    
    try:
        # Gestisci tmpdir come string o oggetto TemporaryDirectory
        tmpdir_path = tmpdir.name if hasattr(tmpdir, "name") else str(tmpdir)
        
        # Ottieni lista file nel folder
        try:
            file_list = folder_obj.list_paths_in_partition()
        except Exception:
            file_list = []
        
        # 1) Match esatto per il percorso
        if file_path in file_list:
            local_path = os.path.join(tmpdir_path, os.path.basename(file_path))
            with folder_obj.get_download_stream(file_path) as stream, open(local_path, 'wb') as out:
                out.write(stream.read())
            
            # Se è uno shapefile, scarica anche i file accessori
            if file_path.lower().endswith('.shp'):
                base_name = os.path.splitext(file_path)[0]
                shapefile_extensions = ['.dbf', '.shx', '.prj', '.qix', '.xml', '.cpg', '.sbx', '.sbn']
                
                for ext in shapefile_extensions:
                    aux_file = base_name + ext
                    if aux_file in file_list:
                        aux_local_path = os.path.join(tmpdir_path, os.path.basename(aux_file))
                        try:
                            with folder_obj.get_download_stream(aux_file) as stream, open(aux_local_path, 'wb') as out:
                                out.write(stream.read())
                        except Exception:
                            continue
            
            return local_path
        
        # 2) Match per nome file (case-insensitive)
        filename = os.path.basename(file_path)
        for f in file_list:
            if os.path.basename(f).lower() == filename.lower():
                local_path = os.path.join(tmpdir_path, os.path.basename(f))
                with folder_obj.get_download_stream(f) as stream, open(local_path, 'wb') as out:
                    out.write(stream.read())
                
                # Se è uno shapefile, scarica anche i file accessori
                if f.lower().endswith('.shp'):
                    base_name = os.path.splitext(f)[0]
                    shapefile_extensions = ['.dbf', '.shx', '.prj', '.qix', '.xml', '.cpg', '.sbx', '.sbn']
                    
                    for ext in shapefile_extensions:
                        aux_file = base_name + ext
                        if aux_file in file_list or aux_file.lower() in [x.lower() for x in file_list]:
                            # Trova il file con case corretto
                            actual_aux_file = next((x for x in file_list if x.lower() == aux_file.lower()), None)
                            if actual_aux_file:
                                aux_local_path = os.path.join(tmpdir_path, os.path.basename(actual_aux_file))
                                try:
                                    with folder_obj.get_download_stream(actual_aux_file) as stream, open(aux_local_path, 'wb') as out:
                                        out.write(stream.read())
                                except Exception:
                                    continue
                
                return local_path
                
        return None
        
    except Exception as e:
        print(f"⚠️ Errore download {file_path}: {e}")
        return None

print("✅ Funzioni payload integrate e Sistema Error Handling definito")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# CLASSE CONFIGURAZIONE FLOOD ANALYSIS
class FloodAnalysisConfig:
    """
    Configurazione centralizzata per analisi sommersione edifici.
    
    Gestisce parametri operativi, validazione input e compatibilità
    con sistema payload per automazione.
    """
    
    def __init__(self, payload: dict = None):
        """
        Inizializza configurazione con priorità:
        1. Scenario JSON (se presente) - PRIORITÀ MASSIMA
        2. Dataset Dataiku 'configurazione_parametri' (primo fallback) - PRIORITÀ MEDIA
        3. Defaults classe (solo se payload e dataset Dataiku mancanti) - PRIORITÀ MINIMA
        
        Nota: Lo scenario JSON sovrascrive COMPLETAMENTE i dataset Dataiku
        """
        
        # DEFAULTS SOLO COME FALLBACK (non sovrascrivono dataset Dataiku)
        self.HEIGHT_FIELD = None       # Deve essere definito nel dataset!
        self.REPROJECTION_OPTION = None  # Deve essere definito nel dataset!
        self.TARGET_EPSG = None        # Deve essere definito nel dataset!
        self.BUFFER_DISTANCE = None   # Deve essere definito nel dataset!
        
        # Formati supportati
        self.VECTOR_EXTENSIONS = ['.shp', '.geojson', '.json', '.gpkg', '.parquet', '.geoparquet', '.kml', '.gml']
        self.RASTER_EXTENSIONS = ['.tif', '.tiff', '.img', '.jp2', '.png', '.jpg', '.jpeg', '.bmp', '.gif']
        
        # Parametri processing
        self.MIN_VALID_HEIGHT = 3.0    # Altezza minima valida (m)
        self.MAX_SUBMERSION_PERCENT = 100.0  # Cap percentuale sommersione
        self.PROGRESS_INTERVAL = 100   # Ogni quanti edifici mostrare progresso
        
        # File management
        self.SUPPORTED_FILE_TYPES = {
            'vettoriale': self.VECTOR_EXTENSIONS,
            'raster': self.RASTER_EXTENSIONS
        }
        
        # Metadati sistema
        self.ELAB_ID = None
        self.EVENT_NAME = None
        self.INPUT_VECTOR_FILE = None
        self.INPUT_RASTER_FILE = None
        self.OUTPUT_FOLDER = "output_inondazioni"
        
        # Sistema logging e output
        self.ENABLE_LOGGING = True
        self.CREATE_REPORT = True
        self.CREATE_SHAPEFILE = True
        
        # Parametri naming personalizzato
        self.OUTPUT_DATASET_NAME = None      # Nome dataset output personalizzato
        self.OUTPUT_FOLDER_NAME = None       # Nome folder output personalizzato  
        self.OUTPUT_FILE_PREFIX = None       # Prefisso file output
        self.OUTPUT_FILE_SUFFIX = None       # Suffisso file output
        self.INCLUDE_TIMESTAMP = True        # Include timestamp nei nomi
        
        # Dataiku integration
        self._dataiku_available = False
        self._dataiku = None
        try:
            import dataiku
            self._dataiku_available = True
            self._dataiku = dataiku
        except ImportError:
            pass
        
        # Carica da payload se fornito
        if payload:
            self._prepare_scenario_payload(payload)
    
    def _prepare_scenario_payload(self, payload: dict):
        """Carica parametri dal payload implementando gerarchia di priorità:
        1. Scenario JSON (payload) - priorità più alta
        2. Dataset Dataiku - priorità media  
        3. Valori di default - priorità più bassa
        """
        mapping = {
            "HEIGHT_FIELD": "HEIGHT_FIELD",
            "REPROJECTION_OPTION": "REPROJECTION_OPTION", 
            "TARGET_EPSG": "TARGET_EPSG",
            "BUFFER_DISTANCE": "BUFFER_DISTANCE",
            "elab_id": "ELAB_ID",
            "event_name": "EVENT_NAME",
            "min_valid_height": "MIN_VALID_HEIGHT",
            "enable_logging": "ENABLE_LOGGING",
            "create_report": "CREATE_REPORT",
            "create_shapefile": "CREATE_SHAPEFILE"
        }
        
        # FASE 1: Carica parametri dal JSON scenario (priorità più alta)
        for payload_key, attr in mapping.items():
            if payload_key in payload:
                val = payload[payload_key]
                
                # Conversioni specifiche
                if attr == "REPROJECTION_OPTION":
                    setattr(self, attr, int(val))
                elif attr == "BUFFER_DISTANCE":
                    setattr(self, attr, None if str(val).lower() == "auto" else float(val))
                elif attr in ("ENABLE_LOGGING", "CREATE_REPORT", "CREATE_SHAPEFILE"):
                    setattr(self, attr, self._to_bool(val))
                else:
                    setattr(self, attr, val)
        
        # FASE 2: Integra con dataset Dataiku per parametri mancanti (priorità media)
        if self._dataiku_available:
            self._integrate_with_dataiku_datasets()
        
        # Estrai file da payload
        files = payload.get('files', {})
        self.INPUT_VECTOR_FILE = files.get('vettoriale')
        self.INPUT_RASTER_FILE = files.get('raster')
        
        # FASE 3: Gestisci parametri di naming personalizzato
        self._load_output_naming(payload)
    
    def _integrate_with_dataiku_datasets(self):
        """Integra configurazione con dataset Dataiku per parametri mancanti"""
        try:
            # Carica parametri da dataset configurazione_parametri
            conf_params = self._dataiku.Dataset("configurazione_parametri").get_dataframe()
            
            # Mappa parametri dai dataset solo se non sono già stati impostati dal JSON
            param_mapping = {
                "HEIGHT_FIELD": "HEIGHT_FIELD",
                "REPROJECTION_OPTION": "REPROJECTION_OPTION",
                "TARGET_EPSG": "TARGET_EPSG",
                "BUFFER_DISTANCE": "BUFFER_DISTANCE",
                "EVENT_NAME": "event_name",
                "MIN_VALID_HEIGHT": "min_valid_height"
            }
            
            for _, row in conf_params.iterrows():
                var_name = row['variabile']
                var_value = row['valore']
                
                # Mappa solo se il parametro corrispondente non è già impostato
                if var_name in param_mapping:
                    attr_name = param_mapping[var_name]
                    current_value = getattr(self, attr_name)
                    
                    # Applica solo se il valore corrente è None o valore di default
                    if self._should_override_with_dataiku(attr_name, current_value):
                        if attr_name == "REPROJECTION_OPTION":
                            setattr(self, attr_name, int(var_value))
                        elif attr_name == "BUFFER_DISTANCE":
                            setattr(self, attr_name, None if str(var_value).lower() == "auto" else float(var_value))
                        else:
                            setattr(self, attr_name, var_value)
                            
        except Exception as e:
            # Se non riesce a caricare i dataset, continua con i valori attuali
            pass
    
    def _should_override_with_dataiku(self, attr_name: str, current_value):
        """Determina se un valore dovrebbe essere sovrascritto dai dataset Dataiku"""
        # Valori di default che possono essere sovrascritti
        default_values = {
            "HEIGHT_FIELD": None,
            "REPROJECTION_OPTION": None,
            "TARGET_EPSG": None,
            "BUFFER_DISTANCE": None,
            "EVENT_NAME": None,
            "MIN_VALID_HEIGHT": 3.0
        }
        
        # Sovrascrivi solo se il valore corrente è None o è il valore di default
        return current_value is None or current_value == default_values.get(attr_name)
    
    def _load_output_naming(self, payload: dict):
        """Carica parametri di naming personalizzato dall'output_naming del payload"""
        output_naming = payload.get('output_naming', {})
        
        if output_naming:
            # Mapping diretto per parametri di naming
            naming_mapping = {
                'dataset_name': 'OUTPUT_DATASET_NAME',
                'folder_name': 'OUTPUT_FOLDER_NAME', 
                'file_prefix': 'OUTPUT_FILE_PREFIX',
                'file_suffix': 'OUTPUT_FILE_SUFFIX',
                'include_timestamp': 'INCLUDE_TIMESTAMP'
            }
            
            for json_key, attr_name in naming_mapping.items():
                if json_key in output_naming:
                    val = output_naming[json_key]
                    if attr_name == 'INCLUDE_TIMESTAMP':
                        setattr(self, attr_name, self._to_bool(val))
                    else:
                        setattr(self, attr_name, val)
    
    def _to_bool(self, val):
        """Converte valori in booleano in modo sicuro"""
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return bool(val)
        if isinstance(val, str):
            return val.lower() in ('true', '1', 'yes', 'on')
        return False
    
    def get_output_names(self):
        """Genera nomi di output personalizzati basati sui parametri di naming"""
        from datetime import datetime
        
        # Timestamp base
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if self.INCLUDE_TIMESTAMP else ""
        
        # Nome dataset output
        if self.OUTPUT_DATASET_NAME:
            dataset_name = self.OUTPUT_DATASET_NAME
            if self.INCLUDE_TIMESTAMP and timestamp:
                dataset_name = f"{dataset_name}_{timestamp}"
        else:
            # Nome default con eventuale timestamp
            dataset_name = f"output_inondazioni_{timestamp}" if timestamp else "output_inondazioni"
        
        # Nome folder output  
        if self.OUTPUT_FOLDER_NAME:
            folder_name = self.OUTPUT_FOLDER_NAME
            if self.INCLUDE_TIMESTAMP and timestamp:
                folder_name = f"{folder_name}_{timestamp}"
        else:
            # Nome default con eventuale timestamp
            folder_name = f"output_inondazioni_{timestamp}" if timestamp else "output_inondazioni"
        
        # Pattern per file individuali
        file_pattern = ""
        if self.OUTPUT_FILE_PREFIX:
            file_pattern += self.OUTPUT_FILE_PREFIX
        if timestamp and self.INCLUDE_TIMESTAMP:
            file_pattern += f"analysis_{timestamp}"
        else:
            file_pattern += "analysis"
        if self.OUTPUT_FILE_SUFFIX:
            file_pattern += f"_{self.OUTPUT_FILE_SUFFIX}"
        
        return {
            'dataset_name': dataset_name,
            'folder_name': folder_name, 
            'file_pattern': file_pattern,
            'timestamp': timestamp
        }
    
    def validate_config(self):
        """Valida configurazione e ritorna lista errori"""
        errors = []
        
        if not self.HEIGHT_FIELD:
            errors.append("HEIGHT_FIELD deve essere specificato")
        
        if self.REPROJECTION_OPTION not in [1, 2, 3]:
            errors.append("REPROJECTION_OPTION deve essere 1, 2 o 3")
        
        if not self.TARGET_EPSG:
            errors.append("TARGET_EPSG deve essere specificato")
        
        if self.BUFFER_DISTANCE is not None and self.BUFFER_DISTANCE <= 0:
            errors.append("BUFFER_DISTANCE deve essere > 0 o None (automatico)")
            
        return errors
    
    def print_config(self):
        """Visualizza configurazione corrente"""
        print(f"\n=== CONFIGURAZIONE FLOOD ANALYSIS ===")
        print(f"Elaborazione ID: {self.ELAB_ID or 'N/A'}")
        print(f"Nome evento: {self.EVENT_NAME or 'N/A'}")
        print(f"Campo altezza: {self.HEIGHT_FIELD}")
        print(f"Opzione riproiezione: {self.REPROJECTION_OPTION}")
        print(f"Target EPSG: {self.TARGET_EPSG}")
        print(f"Buffer distance: {self.BUFFER_DISTANCE or 'automatico'}")
        print(f"Altezza minima valida: {self.MIN_VALID_HEIGHT}m")
        print(f"File vettoriale: {self.INPUT_VECTOR_FILE or 'N/A'}")
        print(f"File raster: {self.INPUT_RASTER_FILE or 'N/A'}")
        print(f"Output folder: {self.OUTPUT_FOLDER}")
        print(f"--- Controlli di output ---")
        print(f"Logging attivo: {self.ENABLE_LOGGING}")
        print(f"Creazione report: {self.CREATE_REPORT}")
        print(f"Creazione shapefile: {self.CREATE_SHAPEFILE}")
        
        # Mostra naming personalizzato se configurato
        if any([self.OUTPUT_DATASET_NAME, self.OUTPUT_FOLDER_NAME, self.OUTPUT_FILE_PREFIX, self.OUTPUT_FILE_SUFFIX]):
            print(f"--- Naming personalizzato ---")
            if self.OUTPUT_DATASET_NAME:
                print(f"Nome dataset custom: {self.OUTPUT_DATASET_NAME}")
            if self.OUTPUT_FOLDER_NAME:
                print(f"Nome folder custom: {self.OUTPUT_FOLDER_NAME}")
            if self.OUTPUT_FILE_PREFIX:
                print(f"Prefisso file: {self.OUTPUT_FILE_PREFIX}")
            if self.OUTPUT_FILE_SUFFIX:
                print(f"Suffisso file: {self.OUTPUT_FILE_SUFFIX}")
            print(f"Include timestamp: {self.INCLUDE_TIMESTAMP}")
            
            # Mostra preview dei nomi generati
            output_names = self.get_output_names()
            print(f"📝 Preview nomi output:")
            print(f"  Dataset: {output_names['dataset_name']}")
            print(f"  Folder: {output_names['folder_name']}")
            print(f"  Pattern file: {output_names['file_pattern']}")
        
        print(f"--- Sistema ---")
        print(f"Dataiku disponibile: {self._dataiku_available}")
        print(f"Formati vettoriali: {len(self.VECTOR_EXTENSIONS)} supportati")
        print(f"Formati raster: {len(self.RASTER_EXTENSIONS)} supportati")
        print("=" * 50)

print("✅ FloodAnalysisConfig definita")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def main():
    """
    Funzione principale per analisi sommersione edifici - Flood Analysis
    """
    print("🚀 AVVIO ANALISI SOMMERSIONE EDIFICI")
    
    try:
        # =================================================================
        # FASE 1: CREAZIONE PAYLOAD
        # =================================================================
        print("=== CREAZIONE PAYLOAD ===")
        
        # Rileva automaticamente la modalità di esecuzione 
        payload = _create_payload()
        print(f"📋 Payload ID: {payload.get('elab_id')}")
        
        # Crea configurazione dal payload
        config = FloodAnalysisConfig(payload)
        
        # Validazione configurazione 
        config_errors = config.validate_config()
        if config_errors:
            error_msg = f"❌ ERRORI CONFIGURAZIONE:\n" + "\n".join(f"  - {err}" for err in config_errors)
            raise ValueError(error_msg)
        
        # Visualizza configurazione finale
        config.print_config()
        
        # Legge configurazione dati (file da processare)
        try:
            configurazione_dati = dataiku.Dataset("configurazione_dati")
            configurazione_dati_df = configurazione_dati.get_dataframe()
            
            print("\n=== DATASET CONFIGURAZIONE DATI ===")
            print(f"Righe nel dataset: {len(configurazione_dati_df)}")
            
            # Estrai file configurati
            try:
                shapefile_config = configurazione_dati_df[configurazione_dati_df['tipo_file'] == 'vettoriale']['nome_file'].iloc[0]
                raster_config = configurazione_dati_df[configurazione_dati_df['tipo_file'] == 'raster']['nome_file'].iloc[0]
                
                config.INPUT_VECTOR_FILE = shapefile_config
                config.INPUT_RASTER_FILE = raster_config
                
            except (IndexError, KeyError):
                print("⚠️ File non trovati nel dataset configurazione_dati")
                print(f"💡 Usando file da payload: vettoriale={config.INPUT_VECTOR_FILE}, raster={config.INPUT_RASTER_FILE}")
        
        except Exception as e:
            print(f"⚠️ Dataset configurazione_dati non disponibile: {e}")
            print("💡 Usando configurazione file da payload")
        
        print(f"\n=== FILE CONFIGURATI FINALI ===")
        print(f"Shapefile configurato: {config.INPUT_VECTOR_FILE}")
        print(f"Raster configurato: {config.INPUT_RASTER_FILE}")
        
        # Ritorna configurazione completa
        return {
            'config': config,
            'payload': payload,
            # Backward compatibility
            'HEIGHT_FIELD': config.HEIGHT_FIELD,
            'REPROJECTION_OPTION': config.REPROJECTION_OPTION,
            'TARGET_EPSG': config.TARGET_EPSG,
            'BUFFER_DISTANCE': config.BUFFER_DISTANCE,
            'shapefile_config': config.INPUT_VECTOR_FILE,
            'raster_config': config.INPUT_RASTER_FILE
        }
        
    except Exception as e:
        print(f"❌ ERRORE CRITICO nella configurazione: {str(e)}")
        raise

def _create_payload_from_config_tables():
    """
    Crea payload leggendo dai dataset di configurazione Dataiku
    (seguendo il pattern del notebook geospaziale)
    """
    print("📊 Creazione payload da tabelle di configurazione Dataiku...")
    
    payload = {
        "elab_id": f"flood_{datetime.now(pytz.timezone('Europe/Rome')).strftime('%Y%m%d_%H%M%S')}"
    }
    
    # Leggi parametri da configurazione_parametri
    try:
        conf_params = dataiku.Dataset("configurazione_parametri").get_dataframe()
        for _, row in conf_params.iterrows():
            payload[row['variabile']] = row['valore']
        print(f"✅ Parametri letti: {list(payload.keys())}")
    except Exception as e:
        print(f"⚠️ Errore lettura configurazione_parametri: {e}")
    
    # Leggi file da configurazione_dati
    try:
        conf_data = dataiku.Dataset("configurazione_dati").get_dataframe()
        payload['files'] = {}
        for _, row in conf_data.iterrows():
            payload['files'][row['tipo_file']] = row['nome_file']
        print(f"✅ File configurati: {list(payload.get('files', {}).keys())}")
    except Exception as e:
        print(f"⚠️ Errore lettura configurazione_dati: {e}")
        payload['files'] = {}
    
    return payload

def _create_payload():
    """
    Crea payload per la configurazione del sistema:
    - Se lanciato da scenario → usa parametri scenario  
    - Se lanciato manualmente → legge da dataset di configurazione
    """
    payload = None
    
    try:
        # Rileva se lanciato da scenario
        run_vars = dataiku.get_custom_variables()
        scenario_run_id = run_vars.get('scenarioTriggerRunId')
        
        if scenario_run_id is not None:
            # 🤖 AUTOMAZIONE: Scenario trigger
            print(f"🤖 Rilevata esecuzione da scenario: {scenario_run_id}")
            scenario_params = run_vars.get('scenarioTriggerParams')
            if scenario_params:
                payload = json.loads(scenario_params)
                if payload.get('elab_id') is None:
                    payload["elab_id"] = f"flood_{scenario_run_id.replace('-', '')[:-3]}"
                print("✅ Payload da scenario trigger caricato")
            else:
                print("⚠️ Scenario trigger senza parametri, fallback su dataset")
                payload = _create_payload_from_config_tables()
        else:
            # 👤 MANUALE: Esecuzione da flow
            print("👤 Rilevata esecuzione manuale da flow")
            payload = _create_payload_from_config_tables()
            
    except Exception as e:
        print(f"⚠️ Errore rilevamento modalità esecuzione: {e}")
        print("💡 Fallback su configurazione da dataset")
        payload = _create_payload_from_config_tables()
    
    return payload or {}

print("✅ Funzione main() definita")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# ESECUZIONE MAIN - ANALISI SOMMERSIONE EDIFICI
print("🎯 AVVIO PROCEDURA PRINCIPALE")

# Esegui funzione principale e ottieni configurazione
config_result = main()

# Estrai configurazione
flood_config = config_result['config']
flood_payload = config_result['payload']

# Inizializza error handler
error_handler = ErrorHandler(flood_config)

# Backward compatibility - estrai parametri singoli
HEIGHT_FIELD = flood_config.HEIGHT_FIELD
REPROJECTION_OPTION = flood_config.REPROJECTION_OPTION  
TARGET_EPSG = flood_config.TARGET_EPSG
BUFFER_DISTANCE = flood_config.BUFFER_DISTANCE
shapefile_config = flood_config.INPUT_VECTOR_FILE
raster_config = flood_config.INPUT_RASTER_FILE

print(f"\n✅ CONFIGURAZIONE COMPLETATA")
print(f"📋 Payload ID: {flood_payload.get('elab_id')}")
print(f"🔧 Error Handler inizializzato")
print(f"⚙️  FloodAnalysisConfig pronta")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## 2. Carica Dati di Input da Dataiku
# 
# Lettura dei parametri di configurazione dal dataset e accesso ai file vettoriali e raster dal folder Minio.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Accesso al folder di input contenente i file di analisi
minio_input = dataiku.Folder("minio_input")

print("=== INFORMAZIONI FOLDER INPUT ===")
print(f"Folder minio_input: {minio_input.get_info()}")

# Elenco dei file disponibili nel folder
input_files = minio_input.list_paths_in_partition()

print(f"\nFile disponibili nel folder minio_input:")
for file_path in input_files:
    print(f"  - {file_path}")

# DEFINIZIONE FORMATI SUPPORTATI
# Formati vettoriali supportati da GeoPandas/Fiona
VECTOR_EXTENSIONS = ['.shp', '.geojson', '.json', '.gpkg', '.parquet', '.geoparquet', '.kml', '.gml']
# Formati raster supportati da Rasterio/GDAL  
RASTER_EXTENSIONS = ['.tif', '.tiff', '.img', '.jp2', '.png', '.jpg', '.jpeg', '.bmp', '.gif']

# Classificazione dei file per tipologia con supporto multi-formato
vector_files = []
raster_files = []

for file_path in input_files:
    file_lower = file_path.lower()
    
    # Controlla formati vettoriali
    if any(file_lower.endswith(ext) for ext in VECTOR_EXTENSIONS):
        vector_files.append(file_path)
    
    # Controlla formati raster
    elif any(file_lower.endswith(ext) for ext in RASTER_EXTENSIONS):
        raster_files.append(file_path)

print(f"\n=== RIEPILOGO FILE PER TIPOLOGIA ===")
print(f"File vettoriali trovati: {len(vector_files)}")
for vec in vector_files:
    file_ext = '.' + vec.split('.')[-1].upper()
    print(f"  - {vec} [{file_ext}]")

print(f"File raster trovati: {len(raster_files)}")
for ras in raster_files:
    file_ext = '.' + ras.split('.')[-1].upper()
    print(f"  - {ras} [{file_ext}]")

# Verifica presenza dei file necessari
if len(vector_files) == 0:
    supported_vec = ', '.join(VECTOR_EXTENSIONS)
    raise ValueError(f"❌ Nessun file vettoriale trovato nel folder minio_input!\n"
                    f"Formati supportati: {supported_vec}")
                    
if len(raster_files) == 0:
    supported_ras = ', '.join(RASTER_EXTENSIONS)
    raise ValueError(f"❌ Nessun file raster trovato nel folder minio_input!\n"
                    f"Formati supportati: {supported_ras}")

print(f"\n📋 Formati vettoriali supportati: {', '.join(VECTOR_EXTENSIONS)}")
print(f"📋 Formati raster supportati: {', '.join(RASTER_EXTENSIONS[:8])}... (+{len(RASTER_EXTENSIONS)-8} altri)")

# SELEZIONE INTELLIGENTE MIGLIORATA CON PAYLOAD
def find_configured_file(file_list, configured_path, file_type):
    """Trova il file che corrisponde alla configurazione con percorso completo"""
    if not configured_path:
        if file_list:
            print(f"⚠️ Nessun {file_type} configurato, uso il primo disponibile: {file_list[0]}")
            return file_list[0]
        return None
    
    configured_filename = configured_path.split('/')[-1]
    
    # 1. Match esatto per nome file
    exact_matches = [f for f in file_list if configured_filename in f]
    if exact_matches:
        print(f"✓ Trovato {file_type} configurato '{configured_filename}': {exact_matches[0]}")
        return exact_matches[0]
    
    # 2. Match parziale nel percorso
    path_matches = [f for f in file_list if configured_path.replace('/', '\\') in f or configured_path.replace('\\', '/') in f]
    if path_matches:
        print(f"✓ Trovato {file_type} con percorso '{configured_path}': {path_matches[0]}")
        return path_matches[0]
    
    # 3. Fallback robusto
    if file_list:
        print(f"⚠️ {file_type} configurato '{configured_path}' non trovato, uso il primo disponibile: {file_list[0]}")
        print(f"   - File cercato: {configured_filename}")
        return file_list[0]
    
    return None

print(f"\n=== SELEZIONE BASATA SU CONFIGURAZIONE ===")
print(f"📁 File vettoriale configurato: {shapefile_config}")
print(f"📁 File raster configurato: {raster_config}")

# Seleziona i file basandosi sulla configurazione (ora con nomi generici)
vector_file = find_configured_file(vector_files, shapefile_config, "file vettoriale")
raster_file = find_configured_file(raster_files, raster_config, "file raster")

print(f"\n=== FILE SELEZIONATI PER L'ANALISI ===\n")
print(f"📄 File vettoriale: {vector_file}")
print(f"🗺️  File raster: {raster_file}")

# Mostra file alternativi disponibili
if len(vector_files) > 1:
    print(f"\n📋 Altri file vettoriali disponibili:")
    for vec in vector_files:
        if vec != vector_file:
            file_ext = '.' + vec.split('.')[-1].upper()
            print(f"   - {vec} [{file_ext}]")

if len(raster_files) > 1:
    print(f"\n📋 Altri file raster disponibili:")
    for ras in raster_files:
        if ras != raster_file:
            file_ext = '.' + ras.split('.')[-1].upper()
            print(f"   - {ras} [{file_ext}]")

print(f"\n💡 Per cambiare selezione, modifica il dataset 'configurazione_dati'")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Download dei file dal folder di input verso directory temporanea locale
temp_dir = tempfile.mkdtemp()
print(f"Directory temporanea creata: {temp_dir}")

# Download con funzione robusta migliorata
print(f"📥 Download in corso: {vector_file}")
try:
    vector_local_path = _download_remote_to_tmp(vector_file, minio_input, temp_dir)
    if vector_local_path:
        print(f"✅ Vector file scaricato: {os.path.basename(vector_local_path)}")
    else:
        raise Exception(f"Download fallito per {vector_file}")
except Exception as e:
    print(f"❌ Errore download vector: {e}")
    raise

print(f"📥 Download in corso: {raster_file}")
try:
    raster_local_path = _download_remote_to_tmp(raster_file, minio_input, temp_dir)
    if raster_local_path:
        print(f"✅ Raster file scaricato: {os.path.basename(raster_local_path)}")
    else:
        raise Exception(f"Download fallito per {raster_file}")
except Exception as e:
    print(f"❌ Errore download raster: {e}")
    raise

# Informazioni sui file scaricati
if vector_local_path.lower().endswith('.shp'):
    print(f"📁 Shapefile completo scaricato (con file accessori)")
elif vector_local_path.lower().endswith(('.gpkg', '.gdb')):
    print(f"📁 File vettoriale database rilevato - formato autocontenuto")
elif vector_local_path.lower().endswith(('.parquet', '.geoparquet')):
    print(f"📁 File GeoParquet rilevato - formato colonnare ottimizzato")
else:
    vector_ext = vector_local_path.split('.')[-1].upper()
    print(f"📁 File vettoriale {vector_ext} - formato autocontenuto")

# Informazioni sui file raster
raster_ext = raster_local_path.split('.')[-1].upper() 
print(f"📁 File raster {raster_ext} scaricato")

print(f"\n=== DOWNLOAD COMPLETATO ===")
print(f"File vettoriale: {vector_local_path}")
print(f"File raster: {raster_local_path}")
print(f"Directory di lavoro: {temp_dir}")
print(f"Formato vettoriale: {vector_local_path.split('.')[-1].upper()}")
print(f"Formato raster: {raster_local_path.split('.')[-1].upper()}")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Carica dati vettoriali e raster con geopandas e rasterio
vector = gpd.read_file(vector_local_path)
raster = rasterio.open(raster_local_path)

print("=== DATI CARICATI ===")
print(f"Edifici nel vettoriale: {len(vector)}")
print(f"Dimensioni raster: {raster.width} x {raster.height}")
print(f"CRS vettoriale: {vector.crs}")
print(f"CRS raster: {raster.crs}")

# Controlla i campi disponibili nel vettoriale
print(f"\nCampi disponibili nel vettoriale:")
print(list(vector.columns))

# Verifica che il campo altezza sia presente
if HEIGHT_FIELD not in vector.columns:
    raise ValueError(f"Campo altezza '{HEIGHT_FIELD}' non trovato nel vettoriale! Campi disponibili: {list(vector.columns)}")
    
print(f"\n✓ Campo altezza '{HEIGHT_FIELD}' trovato nel vettoriale")

# Rilevamento campo FID (case-insensitive)
FID_FIELD = None
fid_value_source = None
for col in vector.columns:
    if col.upper() == 'FID':
        FID_FIELD = col
        fid_value_source = 'input'  # Eredita dall'input
        break

if FID_FIELD:
    print(f"✓ Campo FID trovato nell'input: '{FID_FIELD}' - sarà ereditato")
else:
    print("ℹ️  Campo FID non presente nell'input - sarà generato automaticamente")
    fid_value_source = 'generated'  # Genera automaticamente

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## 3. Allineamento Sistemi di Riferimento
# 
# Controllo della compatibilità CRS tra dati vettoriali e raster e implementazione della logica di riproiezione.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Controllo CRS
vector_crs = vector.crs
raster_crs = raster.crs

if vector_crs != raster_crs:
    print(f"⚠️  ATTENZIONE: I sistemi di riferimento non coincidono!")
    print(f"CRS vettoriale: {vector_crs}")
    print(f"CRS raster: {raster_crs}")
    print(f"Applicando opzione di riproiezione: {REPROJECTION_OPTION}")
    
    try:
        if REPROJECTION_OPTION == 1:
            # Riproietta vettoriale nel CRS del raster
            target_crs = raster_crs
            print(f"Riproiettando il vettoriale in {target_crs}...")
            vector = vector.to_crs(target_crs)
            print("✓ Vettoriale riproiettato.")
            
        elif REPROJECTION_OPTION == 2:
            # Riproietta raster nel CRS del vettoriale
            target_crs = vector_crs
            print(f"Riproiettando il raster in {target_crs}...")
            # Crea file temporaneo per raster riproiettato
            temp_raster = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
            temp_raster_path = temp_raster.name
            temp_raster.close()
            
            # Calcola trasformazione
            transform, width, height = calculate_default_transform(
                raster.crs, target_crs, raster.width, raster.height, *raster.bounds)
            
            # Parametri per il nuovo raster
            kwargs = raster.meta.copy()
            kwargs.update({
                'crs': target_crs,
                'transform': transform,
                'width': width,
                'height': height
            })
            
            # Esegui riproiezione
            with rasterio.open(temp_raster_path, 'w', **kwargs) as dst:
                for i in range(1, raster.count + 1):
                    reproject(
                        source=rasterio.band(raster, i),
                        destination=rasterio.band(dst, i),
                        src_transform=raster.transform,
                        src_crs=raster.crs,
                        dst_transform=transform,
                        dst_crs=target_crs,
                        resampling=Resampling.bilinear)
            
            # Chiudi raster originale e apri quello riproiettato
            raster.close()
            raster = rasterio.open(temp_raster_path)
            print("✓ Raster riproiettato.")
            
        elif REPROJECTION_OPTION == 3:
            # Riproietta entrambi nel CRS specificato
            target_crs = f"EPSG:{TARGET_EPSG}"
            print(f"Riproiettando entrambi in {target_crs}...")
            
            # Riproietta vettoriale
            vector = vector.to_crs(target_crs)
            print("✓ Vettoriale riproiettato.")
            
            # Riproietta raster (stesso codice dell'opzione 2)
            temp_raster = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
            temp_raster_path = temp_raster.name
            temp_raster.close()
            
            transform, width, height = calculate_default_transform(
                raster.crs, target_crs, raster.width, raster.height, *raster.bounds)
            
            kwargs = raster.meta.copy()
            kwargs.update({
                'crs': target_crs,
                'transform': transform,
                'width': width,
                'height': height
            })
            
            with rasterio.open(temp_raster_path, 'w', **kwargs) as dst:
                for i in range(1, raster.count + 1):
                    reproject(
                        source=rasterio.band(raster, i),
                        destination=rasterio.band(dst, i),
                        src_transform=raster.transform,
                        src_crs=raster.crs,
                        dst_transform=transform,
                        dst_crs=target_crs,
                        resampling=Resampling.bilinear)
            
            raster.close()
            raster = rasterio.open(temp_raster_path)
            print("✓ Raster riproiettato.")
            
        else:
            raise ValueError(f"Opzione di riproiezione non valida: {REPROJECTION_OPTION}")
            
    except Exception as e:
        raise Exception(f"Errore durante la riproiezione: {e}")

else:
    print("✓ Sistemi di riferimento già compatibili - nessuna riproiezione necessaria")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## 4. Funzioni di Analisi della Profondità dell'Acqua
# 
# Implementazione della funzione `get_external_pixels()` per estrarre i valori di profondità dell'acqua dai pixel immediatamente esterni al perimetro degli edifici.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def get_external_pixels(geom, raster, buffer_distance=None):
    """
    Estrae i valori dei pixel immediatamente esterni al perimetro del poligono
    
    Parametri:
    - geom: geometria del poligono (edificio)  
    - raster: rasterio dataset con profondità acqua
    - buffer_distance: distanza buffer in metri (None = automatico = risoluzione pixel)
    
    Ritorna:
    - numpy array con valori di profondità validi
    """
    try:
        # Disabilita temporaneamente tutti i warning per evitare messaggi di sovrapposizione
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            
            # Se non specificato, usa la risoluzione del raster come buffer
            if buffer_distance is None:
                buffer_distance = abs(raster.transform[0])  # risoluzione pixel
            
            # Crea buffer esterno molto piccolo
            external_buffer = geom.buffer(buffer_distance)
            
            # Crea anello: buffer esterno - poligono originale
            ring = external_buffer.difference(geom)
            
            # Estrai valori raster dall'anello - questa chiamata può generare il warning
            out_image, out_transform = rasterio.mask.mask(raster, [mapping(ring)], crop=True, filled=True)
            data = out_image[0]
            
            # Escludi nodata
            valid_data = data[data != raster.nodata]
            
            return valid_data
        
    except Exception as e:
        # Gestione silenziosa degli errori comuni (es. nessuna sovrapposizione)
        # Gli errori saranno tracciati nel conteggio generale
        return np.array([])

print("✓ Funzione get_external_pixels() definita con soppressione warning")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## 5. Calcolo della Sommersione degli Edifici
# 
# Elaborazione di ogni edificio per calcolare area, volume e statistiche di sommersione con tracking del progresso.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# WORKFLOW MODULARE - FASE PROCESSING EDIFICI
def run_flood_analysis_workflow(config: FloodAnalysisConfig, vector, raster, error_handler: ErrorHandler):
    """
    Workflow modulare per analisi sommersione edifici con error handling robusto
    
    Args:
        config: Configurazione FloodAnalysisConfig
        vector: GeoDataFrame edifici 
        raster: Rasterio dataset profondità acqua
        error_handler: Gestore errori centralizzato
    
    Returns:
        tuple: (results_list, processing_stats)
    """
    
    print(f"🔧 AVVIO WORKFLOW MODULARE - {len(vector)} edifici da processare")
    
    # =================================================================
    # FASE 1: PREPARAZIONE E VALIDAZIONE
    # =================================================================
    print("=== FASE 1: PREPARAZIONE DATI ===")
    
    # Validazione campo altezza
    if config.HEIGHT_FIELD not in vector.columns:
        error_handler.handle_validation_error(
            "MISSING_HEIGHT_FIELD", 
            f"Campo '{config.HEIGHT_FIELD}' non presente. Disponibili: {list(vector.columns)}"
        )
        return [], {}
    
    # Rilevamento campo FID (case-insensitive) - MIGLIORATO
    fid_field = None
    fid_value_source = None
    
    for col in vector.columns:
        if col.upper() == 'FID':
            fid_field = col
            fid_value_source = 'input'
            break
    
    if not fid_field:
        fid_value_source = 'generated'
        print("ℹ️  Campo FID generato automaticamente")
    else:
        print(f"✅ Campo FID ereditato: '{fid_field}'")
    
    # Statistiche pre-processing
    total_buildings = len(vector)
    valid_heights = (vector[config.HEIGHT_FIELD] > config.MIN_VALID_HEIGHT).sum()
    invalid_heights = total_buildings - valid_heights
    
    print(f"📊 Edifici totali: {total_buildings}")
    print(f"📊 Altezze valide (>{config.MIN_VALID_HEIGHT}m): {valid_heights}")
    print(f"📊 Altezze non valide: {invalid_heights}")
    
    if invalid_heights > 0:
        error_handler.add_warning(
            "INVALID_HEIGHTS", 
            f"{invalid_heights} edifici con altezza ≤ {config.MIN_VALID_HEIGHT}m saranno saltati"
        )
    
    # =================================================================
    # FASE 2: PROCESSING EDIFICI CON ERROR HANDLING
    # =================================================================
    print(f"\n=== FASE 2: PROCESSING EDIFICI ===")
    
    results = []
    stats = {
        'processed_count': 0,
        'skipped_invalid_height': 0,
        'skipped_no_overlap': 0,
        'skipped_geometry_error': 0,
        'skipped_other_error': 0
    }
    
    # Loop principale con error handling robusto
    for idx, row in vector.iterrows():
        building_id = f"building_{idx}"
        
        try:
            # Pre-validazione altezza
            geom = row.geometry
            h_uvl = row[config.HEIGHT_FIELD]
            
            if h_uvl <= config.MIN_VALID_HEIGHT:
                stats['skipped_invalid_height'] += 1
                # Crea record con valori zero per altezze non valide
                result_record = _create_empty_result_record(
                    idx=idx, 
                    row=row, 
                    config=config,
                    fid_field=fid_field,
                    fid_value_source=fid_value_source,
                    reason="invalid_height"
                )
                results.append(result_record)
                continue
            
            # Calcoli geometrici base
            a_base = geom.area
            vol = a_base * h_uvl
            
            # Estrazione valori con error handling
            try:
                external_values = get_external_pixels(geom, raster, config.BUFFER_DISTANCE)
                
                if external_values.size > 0:
                    # Calcola statistiche sommersione
                    depth_mean = float(np.mean(external_values))
                    depth_min = float(np.min(external_values))
                    depth_max = float(np.max(external_values))
                    
                    # Calcola percentuale sommersione con cap
                    perc_submerged = min((depth_mean / h_uvl) * 100, config.MAX_SUBMERSION_PERCENT)
                    
                    stats['processed_count'] += 1
                    
                else:
                    # Nessuna sovrapposizione
                    stats['skipped_no_overlap'] += 1
                    depth_mean = depth_min = depth_max = perc_submerged = 0.0
                
            except Exception as e:
                # Errore nell'estrazione pixel
                error_handler.handle_processing_error(building_id, "pixel_extraction", e)
                stats['skipped_other_error'] += 1
                depth_mean = depth_min = depth_max = perc_submerged = 0.0
            
            # Crea record risultato
            result_record = _create_result_record(
                idx=idx,
                row=row,
                config=config,
                fid_field=fid_field,
                fid_value_source=fid_value_source,
                a_base=a_base,
                h_uvl=h_uvl,
                vol=vol,
                depth_mean=depth_mean,
                depth_min=depth_min,
                depth_max=depth_max,
                perc_submerged=perc_submerged,
                geom=geom
            )
            
            results.append(result_record)
            
        except Exception as e:
            # Errore generale processing edificio
            error_handler.handle_processing_error(building_id, "general_processing", e)
            stats['skipped_other_error'] += 1
            
            # Crea record vuoto per mantenere consistenza
            try:
                empty_record = _create_empty_result_record(
                    idx=idx,
                    row=row, 
                    config=config,
                    fid_field=fid_field,
                    fid_value_source=fid_value_source,
                    reason="processing_error"
                )
                results.append(empty_record)
            except:
                pass  # Fallback silenzioso
        
        # Progress indicator
        if (idx + 1) % config.PROGRESS_INTERVAL == 0:
            print(f"📊 Elaborati {idx + 1}/{total_buildings} edifici...")
    
    # =================================================================
    # FASE 3: SUMMARY E VALIDAZIONE FINALE
    # =================================================================
    print(f"\n=== FASE 3: SUMMARY RISULTATI ===")
    print(f"✅ Edifici processati con successo: {stats['processed_count']}")
    print(f"⚠️  Edifici saltati per altezza non valida: {stats['skipped_invalid_height']}")
    print(f"⚠️  Edifici saltati per mancanza sovrapposizione: {stats['skipped_no_overlap']}")
    print(f"❌ Edifici saltati per errori geometrici: {stats['skipped_geometry_error']}")
    print(f"❌ Edifici saltati per altri errori: {stats['skipped_other_error']}")
    print(f"📊 Record risultato totali: {len(results)}")
    
    return results, stats

def _create_result_record(idx, row, config, fid_field, fid_value_source, 
                         a_base, h_uvl, vol, depth_mean, depth_min, depth_max, 
                         perc_submerged, geom):
    """Crea record risultato standard"""
    
    # Gestione FID
    if fid_value_source == 'input':
        fid_value = row[fid_field]
    else:
        fid_value = idx + 1
    
    return {
        'FID': fid_value,
        'A_BASE': round(float(a_base), 2),
        config.HEIGHT_FIELD: round(float(h_uvl), 2),
        'VOL': round(float(vol), 2),
        'DEPTH_MEAN': round(float(depth_mean), 2),
        'DEPTH_MIN': round(float(depth_min), 2),
        'DEPTH_MAX': round(float(depth_max), 2),
        'PERC_SUBM': round(float(perc_submerged), 2),
        'geometry': geom
    }

def _create_empty_result_record(idx, row, config, fid_field, fid_value_source, reason="unknown"):
    """Crea record vuoto per edifici non processabili"""
    
    try:
        geom = row.geometry
        a_base = geom.area
        h_uvl = row[config.HEIGHT_FIELD] if config.HEIGHT_FIELD in row else 0.0
        vol = a_base * h_uvl if h_uvl > 0 else 0.0
    except:
        # Fallback estremo
        geom = row.geometry if hasattr(row, 'geometry') else None
        a_base = h_uvl = vol = 0.0
    
    # Gestione FID
    if fid_value_source == 'input' and fid_field and fid_field in row:
        fid_value = row[fid_field]
    else:
        fid_value = idx + 1
    
    return {
        'FID': fid_value,
        'A_BASE': round(float(a_base), 2),
        config.HEIGHT_FIELD: round(float(h_uvl), 2),
        'VOL': round(float(vol), 2),
        'DEPTH_MEAN': 0.0,
        'DEPTH_MIN': 0.0,
        'DEPTH_MAX': 0.0,
        'PERC_SUBM': 0.0,
        'geometry': geom
    }

print("✅ Workflow modulare robusto definito")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# ESECUZIONE WORKFLOW MODULARE
print("🚀 AVVIO WORKFLOW AVANZATO")

# Verifica che error_handler possa continuare (no errori critici)
if not error_handler.should_continue_processing():
    print("❌ ERRORI CRITICI RILEVATI - Interrompo processing")
    error_handler.print_final_report()
    raise FloodAnalysisError("Processing interrotto per errori critici nella configurazione")

# Esegui workflow modulare con configurazione avanzata
results, processing_stats = run_flood_analysis_workflow(
    config=flood_config,
    vector=vector,
    raster=raster, 
    error_handler=error_handler
)

# Aggiorna variabili per backward compatibility
processed_count = processing_stats['processed_count']
not_processed_count = (
    processing_stats['skipped_invalid_height'] + 
    processing_stats['skipped_no_overlap'] + 
    processing_stats['skipped_geometry_error'] + 
    processing_stats['skipped_other_error']
)
no_overlap_count = processing_stats['skipped_no_overlap']
zero_height_count = processing_stats['skipped_invalid_height']
other_errors_count = processing_stats['skipped_other_error']

print(f"\n🎯 WORKFLOW AVANZATO COMPLETATO")
print(f"📊 Statistiche processing aggiornate:")
print(f"  - Successi: {processed_count}")
print(f"  - Fallimenti: {not_processed_count}")
print(f"  - Record totali: {len(results)}")

# Report errori finale
error_handler.print_final_report()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## 6. Preparazione Output
# 
# Creazione del GeoDataFrame di output con i risultati dell'analisi di sommersione elaborati dal workflow avanzato.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Crea GeoDataFrame di output
out_gdf = gpd.GeoDataFrame(results, crs=vector.crs)
total_buildings = len(vector)  # Variabile necessaria per il report

print(f"✓ GeoDataFrame di output pronto: {len(out_gdf)} record con campi analisi aggiunti")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## 7. Generazione Output
# 
# Creazione del DataFrame di output con schema appropriato e scrittura nel dataset Dataiku di destinazione.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Converti GeoDataFrame in DataFrame standard per Dataiku
output_inondazioni_df = pd.DataFrame(out_gdf.drop(columns='geometry'))

# Aggiungi colonna WKT come prima colonna per il CSV
output_inondazioni_df.insert(0, 'geometry_wkt', out_gdf['geometry'].apply(lambda x: x.wkt))

# Assicura che FID sia la seconda colonna
if 'FID' in output_inondazioni_df.columns:
    fid_col = output_inondazioni_df.pop('FID')
    output_inondazioni_df.insert(1, 'FID', fid_col)

print(f"📋 DataFrame per output Dataiku:")
print(f"   Righe: {len(output_inondazioni_df)}")
print(f"   Colonne: {len(output_inondazioni_df.columns)}")
print(f"   Colonne: {list(output_inondazioni_df.columns)}")

# Mostra esempio primi record
print(f"\n📊 Esempio primi 3 record:")
print(output_inondazioni_df.head(3))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Salvataggio risultati come CSV (condizionale)
if flood_config.CREATE_REPORT:
    print("📝 Salvataggio file CSV...")

    italian_tz = pytz.timezone('Europe/Rome')
    timestamp = datetime.now(italian_tz).strftime("%Y%m%d_%H%M%S") 
    csv_filename = f"risultati_inondazioni_{timestamp}.csv"

    try:
        output_folder = dataiku.Folder("output_inondazioni")
        csv_content = output_inondazioni_df.to_csv(index=False)
        
        from io import StringIO
        csv_stream = StringIO(csv_content)
        output_folder.upload_stream(csv_filename, csv_stream.getvalue().encode('utf-8'))
        
        print(f"✅ File CSV salvato: {csv_filename}")
        print(f"✅ {len(output_inondazioni_df)} record salvati")
        
    except Exception as e:
        local_csv = f"C:\\temp\\{csv_filename}"
        output_inondazioni_df.to_csv(local_csv, index=False)
        print(f"✅ File salvato in locale: {local_csv}")
        print(f"✅ {len(output_inondazioni_df)} record salvati")
else:
    print("⏭️ Salvataggio CSV disabilitato (create_report=false)")

print("✅ ELABORAZIONE COMPLETATA")
print(f"✅ Dataset 'output_inondazioni' scritto con {len(output_inondazioni_df)} record")
print(f"✅ Analisi di sommersione completata per {processed_count}/{total_buildings} edifici")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## 8. Salvataggio File Fisici
# 
# Salvataggio di shapefile, report statistico e upload nel folder Dataiku di output.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Configurazione folder di output per il salvataggio dei risultati
output_folder = dataiku.Folder("output_inondazioni")

# Creazione directory temporanea per i file di output
output_temp_dir = tempfile.mkdtemp()
print(f"Directory temporanea per output: {output_temp_dir}")

# Generazione nome base per i file di output con timestamp
italian_tz = pytz.timezone('Europe/Rome')
timestamp = datetime.now(italian_tz).strftime("%Y%m%d_%H%M%S")
output_base_name = f"wd_analysis_{timestamp}"

# Definizione percorsi dei file di output
shapefile_path = os.path.join(output_temp_dir, f"{output_base_name}.shp")
report_path = os.path.join(output_temp_dir, f"{output_base_name}_report.txt")
log_path = os.path.join(output_temp_dir, f"{output_base_name}.log")

print(f"File di output programmati:")
print(f"   Shapefile: {os.path.basename(shapefile_path)}")
print(f"   Report statistico: {os.path.basename(report_path)}")
print(f"   Log elaborazione: {os.path.basename(log_path)}")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Salva vettoriale con schema definito (condizionale)
if flood_config.CREATE_SHAPEFILE:
    schema = {
        'geometry': 'Polygon',
        'properties': {
            'FID': 'int:10',             # Campo identificativo
            'A_BASE': 'float:10.2',      # 10 cifre totali, 2 decimali
            HEIGHT_FIELD: 'float:8.2',   # 8 cifre totali, 2 decimali  
            'VOL': 'float:12.2',         # 12 cifre totali, 2 decimali
            'DEPTH_MEAN': 'float:8.2',   # 8 cifre totali, 2 decimali
            'DEPTH_MIN': 'float:8.2',    # 8 cifre totali, 2 decimali
            'DEPTH_MAX': 'float:8.2',    # 8 cifre totali, 2 decimali
            'PERC_SUBM': 'float:6.2'     # 6 cifre totali, 2 decimali
        }
    }

    print("💾 Salvataggio vettoriale...")
    with fiona.open(shapefile_path, 'w', driver='ESRI Shapefile', crs=out_gdf.crs, schema=schema) as f:
        for idx, row in out_gdf.iterrows():
            feature = {
                'geometry': mapping(row.geometry),
                'properties': {
                    'FID': int(row['FID']),
                    'A_BASE': float(row['A_BASE']),
                    HEIGHT_FIELD: float(row[HEIGHT_FIELD]),
                    'VOL': float(row['VOL']),
                    'DEPTH_MEAN': float(row['DEPTH_MEAN']),
                    'DEPTH_MIN': float(row['DEPTH_MIN']),
                    'DEPTH_MAX': float(row['DEPTH_MAX']),
                    'PERC_SUBM': float(row['PERC_SUBM'])
                }
            }
            f.write(feature)
else:
    print("⏭️ Salvataggio shapefile disabilitato (create_shapefile=false)")
    shapefile_path = None  # Prevent file operations later

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Crea report statistico dettagliato (condizionale)
if flood_config.CREATE_REPORT:
    print("📊 Generazione report statistico...")

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=== REPORT ANALISI SOMMERSIONE EDIFICI ===\n\n")
        f.write(f"Data elaborazione: {datetime.now(pytz.timezone('Europe/Rome')).strftime('%Y-%m-%d %H:%M:%S')} (Ora italiana)\n")
        f.write(f"Versione script: dataiku_integration.ipynb\n")
        f.write(f"Campo altezza utilizzato: {HEIGHT_FIELD}\n")
        f.write(f"Opzione riproiezione: {REPROJECTION_OPTION} ")
        if REPROJECTION_OPTION == 1:
            f.write("(riproietta vettoriale)\n")
        elif REPROJECTION_OPTION == 2:
            f.write("(riproietta raster)\n")
        else:
            f.write(f"(riproietta entrambi in {TARGET_EPSG})\n")
        f.write(f"Buffer distance: {BUFFER_DISTANCE}\n\n")
        
        f.write("=== FILE DI INPUT/OUTPUT ===\n")
        f.write(f"File vettoriale: {vector_file}\n")
        f.write(f"File raster: {raster_file}\n")
        f.write(f"File output shapefile: {os.path.basename(shapefile_path)}\n")
        f.write(f"File output report: {os.path.basename(report_path)}\n\n")
        
        f.write("=== SISTEMI DI RIFERIMENTO ===\n")
        f.write(f"CRS vettoriale originale: {vector_crs}\n")
        f.write(f"CRS raster: {raster_crs}\n")
        if vector_crs != raster_crs:
            f.write("NOTA: Sistemi di riferimento diversi - applicata riproiezione automatica\n")
        else:
            f.write("NOTA: Sistemi di riferimento coincidenti - nessuna riproiezione necessaria\n")
        f.write("\n")
        
        f.write("=== RIEPILOGO ELABORAZIONE ===\n")
        f.write(f"Edifici totali nel vettoriale: {total_buildings}\n")
        f.write(f"Edifici processati con successo: {processed_count} ({processed_count/total_buildings*100:.1f}%)\n")
        f.write(f"Edifici non processati: {not_processed_count} ({not_processed_count/total_buildings*100:.1f}%)\n")
        f.write(f"  - Cause: senza sovrapposizione con raster, altezza zero/negativa, errori geometrici\n\n")
        
        f.write("=== METODOLOGIA ===\n")
        f.write("L'analisi calcola la sommersione degli edifici campionando i valori di profondità\n")
        f.write("dell'acqua nei pixel esterni al perimetro di ciascun edificio.\n")
        f.write("La percentuale di sommersione è calcolata come: (profondità_media / altezza_edificio) × 100\n")
        f.write("I valori sono limitati al 100% per edifici completamente sommersi.\n\n")
        
        # Statistiche dettagliate se ci sono edifici processati
        if processed_count > 0:
            processed_data = out_gdf[out_gdf['DEPTH_MEAN'] > 0]
            
            if len(processed_data) > 0:
                f.write(f"Edifici con sommersione rilevata: {len(processed_data)} ({len(processed_data)/total_buildings*100:.1f}%)\n\n")
                
                # Statistiche profondità
                mean_depth = processed_data['DEPTH_MEAN'].mean()  
                max_depth = processed_data['DEPTH_MAX'].max()
                min_depth = processed_data['DEPTH_MIN'].min()
                
                f.write("=== PROFONDITÀ ACQUA ===\n")
                f.write(f"Profondità media: {mean_depth:.2f} m (range: {min_depth:.2f} - {max_depth:.2f} m)\n\n")
                
                # Classificazione edifici
                edifici_bassi = len(processed_data[processed_data['PERC_SUBM'] < 25])
                edifici_medi = len(processed_data[(processed_data['PERC_SUBM'] >= 25) & (processed_data['PERC_SUBM'] < 75)])
                edifici_alti = len(processed_data[processed_data['PERC_SUBM'] >= 75])
                
                f.write("=== CLASSIFICAZIONE EDIFICI PER LIVELLO SOMMERSIONE ===\n")
                f.write(f"Sommersione bassa (<25%): {edifici_bassi} edifici ({edifici_bassi/len(processed_data)*100:.1f}%)\n")
                f.write(f"Sommersione media (25-75%): {edifici_medi} edifici ({edifici_medi/len(processed_data)*100:.1f}%)\n")
                f.write(f"Sommersione alta (≥75%): {edifici_alti} edifici ({edifici_alti/len(processed_data)*100:.1f}%)\n\n")
        
        f.write("=== CAMPI OUTPUT VETTORIALE ===\n")
        fid_source_text = "ereditato dall'input" if fid_value_source == 'input' else 'generato automaticamente'
        f.write(f"FID: Identificativo univoco edificio ({fid_source_text})\n")
        f.write("DEPTH_MEAN: Profondità media dell'acqua attorno all'edificio (m)\n")
        f.write("DEPTH_MAX: Profondità massima dell'acqua attorno all'edificio (m)\n")
        f.write("DEPTH_MIN: Profondità minima dell'acqua attorno all'edificio (m)\n")
        f.write("PERC_SUBM: Percentuale di sommersione dell'edificio (%)\n")
        f.write(f"{HEIGHT_FIELD}: Altezza dell'edificio utilizzata nel calcolo (m)\n")
        f.write("A_BASE: Area della base dell'edificio (m²)\n")
        f.write("VOL: Volume dell'edificio (m³)\n")

    print(f"✅ Report salvato: {os.path.basename(report_path)}")
else:
    print("⏭️ Generazione report statistico disabilitata (create_report=false)")
    report_path = None  # Prevent file operations later

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Crea il file di log con TUTTE le stampe catturate (condizionale)
if flood_config.ENABLE_LOGGING:
    print("📝 Generazione file di log completo...")
    with open(log_path, 'w', encoding='utf-8') as f:
        f.write(f"=== LOG ELABORAZIONE ANALISI SOMMERSIONE EDIFICI ===\n\n")
        f.write(f"Data elaborazione: {datetime.now(pytz.timezone('Europe/Rome')).strftime('%Y-%m-%d %H:%M:%S')} (Ora italiana)\n")
        f.write(f"Versione script: dataiku_integration.ipynb\n")
        f.write(f"Parametri utilizzati:\n")
        f.write(f"  - HEIGHT_FIELD: {HEIGHT_FIELD}\n")
        f.write(f"  - REPROJECTION_OPTION: {REPROJECTION_OPTION}\n")
        f.write(f"  - TARGET_EPSG: {TARGET_EPSG}\n")
        f.write(f"  - BUFFER_DISTANCE: {BUFFER_DISTANCE}\n")
        f.write(f"\n=== TRANSCRIPT ESECUZIONE ===\n\n")
        f.write(log_capture.get_log_content())
        f.write(f"\n=== FINE LOG ===\n")
else:
    print("⏭️ Generazione log disabilitata (enable_logging=false)")
    log_path = None  # Prevent file operations later

# Upload dei file di output nel folder Dataiku
print("📤 Upload file nel folder Dataiku di output...")

# Lista di tutti i file da caricare (shapefile + accessori + report + log)
files_to_upload = []

# Shapefile principale (se creato)
if shapefile_path and os.path.exists(shapefile_path):
    files_to_upload.append(shapefile_path)
    # File accessori shapefile
    for ext in ['.dbf', '.shx', '.prj', '.cpg']:
        aux_file = shapefile_path.replace('.shp', ext)
        if os.path.exists(aux_file):
            files_to_upload.append(aux_file)

# Report (se creato)
if report_path and os.path.exists(report_path):
    files_to_upload.append(report_path)

# File di log (se abilitato)
if log_path and os.path.exists(log_path):
    files_to_upload.append(log_path)

# Upload dei file
uploaded_files = []
for file_path in files_to_upload:
    if os.path.exists(file_path):
        file_name = os.path.basename(file_path)
        try:
            with open(file_path, 'rb') as f:
                output_folder.upload_stream(file_name, f)
            uploaded_files.append(file_name)
        except Exception as e:
            pass  # Gestione silenziosa degli errori

# Aggiorna il log con le informazioni di upload
with open(log_path, 'a', encoding='utf-8') as f:
    f.write(f"\n=== OPERAZIONI DI UPLOAD ===\n\n")
    f.write(f"📤 Upload completato nel folder 'minio/output'\n")
    f.write(f"📁 File caricati: {len(uploaded_files)}\n")
    for filename in uploaded_files:
        f.write(f"   ✅ {filename}\n")
    f.write(f"\n🎉 SALVATAGGIO COMPLETATO!\n")

# Re-upload del log aggiornato
try:
    with open(log_path, 'rb') as f:
        output_folder.upload_stream(os.path.basename(log_path), f)
except:
    pass

# Pulizia directory temporanea output  
try:
    shutil.rmtree(output_temp_dir)
except Exception as e:
    pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Pulizia risorse e file temporanei
try:
    raster.close()
    if 'temp_raster_path' in locals():
        try:
            os.unlink(temp_raster_path)
        except:
            pass

    # Pulizia directory temporanea
    try:
        shutil.rmtree(temp_dir)
    except:
        pass
    
except Exception as e:
    pass

# Ripristina stdout originale e chiudi il sistema di logging
sys.stdout = log_capture.original_stdout
log_capture.close()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# FINALIZZAZIONE AVANZATA E EXIT CODE
# Determina exit code basato sui risultati con logica avanzata
try:
    # Ottieni summary errori dal error handler
    error_summary = error_handler.get_error_summary()
    
    if 'processed_count' in locals() and processed_count > 0:
        # Successo con eventuali warning
        if error_summary['has_critical_errors']:
            exit_code = 2  # Successo con errori critici
            print(f"⚠️  ANALISI COMPLETATA CON ERRORI CRITICI - Exit Code: {exit_code}")
        elif error_summary['total_errors'] > 0:
            exit_code = 1  # Successo con errori non critici
            print(f"⚠️  ANALISI COMPLETATA CON ERRORI - Exit Code: {exit_code}")
        else:
            exit_code = 0  # Successo completo
            print(f"✅ ANALISI SOMMERSIONE COMPLETATA - Exit Code: {exit_code}")
        
        print(f"📊 Risultati: {processed_count}/{len(vector) if 'vector' in locals() else 0} edifici elaborati con successo")
        print(f"📁 Output generati: CSV, Shapefile, Report e Log")
        print(f"🔧 Errori tracciati: {error_summary['total_errors']}")
        print(f"⚠️  Warning: {error_summary['total_warnings']}")
        
    else:
        exit_code = 3  # Fallimento completo
        print(f"❌ ANALISI FALLITA - Exit Code: {exit_code}")
        print(f"🔥 Nessun edificio elaborato con successo")
        print(f"🔧 Errori critici: {error_summary.get('stats', {}).get('validation_errors', 0)}")
        
except Exception as e:
    exit_code = 4  # Errore critico sistema
    print(f"❌ ERRORE CRITICO SISTEMA - Exit Code: {exit_code}")
    print(f"🔥 Errore: {str(e)}")

# Compatibilità con architettura payload avanzato
print(f"\n🎉 PROCEDURA AVANZATA TERMINATA - Exit Code: {exit_code}")
print(f"📋 Payload ID elaborazione: {flood_payload.get('elab_id') if 'flood_payload' in locals() else 'N/A'}")
print(f"🔧 Config ID: {flood_config.ELAB_ID if 'flood_config' in locals() else 'N/A'}")

# Sistema exit code avanzato:
# 0 = Successo completo
# 1 = Successo con errori non critici  
# 2 = Successo con errori critici
# 3 = Fallimento completo
# 4 = Errore critico sistema

# Per compatibilità futura con script autonomo
if __name__ == "__main__":
    import sys
    sys.exit(exit_code)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## 🧪 Test Scenario JSON - Area di Testing
# 
# **IMPORTANTE**: Questa sezione è solo per testing. Non influenza l'esecuzione normale del notebook.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# 🧪 TESTER SCENARIO JSON
# Questa cella simula l'esecuzione di uno scenario senza modificare il workflow principale

def test_scenario_parsing(test_json_str):
    """
    Testa il parsing di un JSON scenario senza eseguire l'analisi completa
    """
    import json
    
    print("🧪 === TEST SCENARIO JSON ===")
    print(f"JSON di input:\n{test_json_str}\n")
    
    try:
        # Parse JSON
        scenario_params = json.loads(test_json_str)
        print("✅ JSON parsing riuscito")
        
        # Simula creazione payload scenario
        payload = scenario_params.copy()
        if payload.get('elab_id') is None:
            payload["elab_id"] = f"flood_test_{datetime.now(pytz.timezone('Europe/Rome')).strftime('%Y%m%d_%H%M%S')}"
        
        print(f"📋 Payload generato: {list(payload.keys())}")
        
        # Testa FloodAnalysisConfig con il payload
        test_config = FloodAnalysisConfig(payload)
        
        print("\n🔧 === CONFIGURAZIONE RISULTANTE ===")
        test_config.print_config()
        
        # Validazione
        errors = test_config.validate_config()
        if errors:
            print(f"\n❌ ERRORI DI VALIDAZIONE:")
            for error in errors:
                print(f"  - {error}")
        else:
            print(f"\n✅ VALIDAZIONE SUPERATA - Configurazione valida")
            
        print(f"\n📁 File configurati:")
        print(f"  - Vettoriale: {test_config.INPUT_VECTOR_FILE or 'N/A'}")
        print(f"  - Raster: {test_config.INPUT_RASTER_FILE or 'N/A'}")
        
        print(f"\n⚙️ Parametri di controllo:")
        print(f"  - Logging: {test_config.ENABLE_LOGGING}")
        print(f"  - Report: {test_config.CREATE_REPORT}")
        print(f"  - Shapefile: {test_config.CREATE_SHAPEFILE}")
        
        return True
        
    except json.JSONDecodeError as e:
        print(f"❌ ERRORE JSON: {e}")
        return False
    except Exception as e:
        print(f"❌ ERRORE CONFIGURAZIONE: {e}")
        return False

# ===== ESEMPI DI TEST =====

# Test 1: Scenario BASE
print("\n" + "="*60)
test_scenario_1 = '''
{
  "files": {
    "vettoriale": "edifici.shp",
    "raster": "flood_depth.tif"
  }
}
'''
test_scenario_parsing(test_scenario_1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Test 2: Scenario COMPLETO
print("\n" + "="*60)
test_scenario_2 = '''
{
  "elab_id": "flood_test_custom",
  "event_name": "Test Alluvione Scenario",
  "files": {
    "vettoriale": "edifici_roma.shp",
    "raster": "flood_depth_tevere.tif"
  },
  "HEIGHT_FIELD": "h_uvl",
  "BUFFER_DISTANCE": 2.5,
  "TARGET_EPSG": "32632",
  "REPROJECTION_OPTION": 3,
  "min_valid_height": 0.5,
  "create_report": true,
  "create_shapefile": false,
  "enable_logging": true
}
'''
test_scenario_parsing(test_scenario_2)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Test 3: Scenario con ERRORI (per testare validazione)
print("\n" + "="*60)
test_scenario_3 = '''
{
  "files": {
    "vettoriale": "edifici.shp",
    "raster": "flood.tif"
  },
  "HEIGHT_FIELD": "",
  "REPROJECTION_OPTION": 5,
  "BUFFER_DISTANCE": -1.5,
  "create_report": "maybe",
  "min_valid_height": "invalid"
}
'''
test_scenario_parsing(test_scenario_3)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# 🔧 TEST SCENARI PERSONALIZZATI

# Modifica questo JSON per testare i tuoi scenari personalizzati:
your_custom_scenario = '''
{
  "files": {
    "vettoriale": "mio_edifici.shp",
    "raster": "mia_profondita.tif"
  },
  "HEIGHT_FIELD": "altezza_custom",
  "event_name": "Il mio test",
  "create_report": false,
  "enable_logging": false
}
'''

print("\n" + "="*60)
print("🔧 TEST SCENARIO PERSONALIZZATO:")
test_scenario_parsing(your_custom_scenario)

print("\n🎯 === RIEPILOGO TEST COMPLETATO ===")
print("✅ Tutti i test scenario sono stati eseguiti")
print("💡 Modifica il JSON 'your_custom_scenario' per testare altre configurazioni")
print("📋 Ricorda: questi test NON eseguono l'analisi completa, solo il parsing e la validazione")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# 🎨 TEST NAMING PERSONALIZZATO - Verifica sistema di naming custom
import json  # Import necessario per il test

print("🎨" + "="*60)
print("🎨 === TEST NAMING PERSONALIZZATO ===")

# Scenario con naming personalizzato completo
test_scenario_naming = {
    "files": {
        "vettoriale": "edifici_tevere.shp",
        "raster": "flood_depth_tevere.tif"
    },
    "event_name": "Alluvione_Tevere_2024",
    "output_naming": {
        "dataset_name": "risultati_alluvione_tevere",
        "folder_name": "output_tevere_analisi", 
        "file_prefix": "tevere_flood_",
        "file_suffix": "final",
        "include_timestamp": True
    }
}

print("JSON di input:")
print()
print(json.dumps(test_scenario_naming, indent=2))
print()
print()

try:
    # Test configurazione con naming personalizzato
    config_naming = FloodAnalysisConfig(test_scenario_naming)
    
    print("✅ JSON parsing riuscito")
    print(f"📋 Payload generato: {list(test_scenario_naming.keys())}")
    print()
    print("🔧 === CONFIGURAZIONE CON NAMING PERSONALIZZATO ===")
    print()
    
    # Mostra configurazione
    config_naming.print_config()
    
    # Test validazione
    naming_errors = config_naming.validate_config()
    if naming_errors:
        print("❌ ERRORI DI VALIDAZIONE:")
        for err in naming_errors:
            print(f"  - {err}")
    else:
        print("✅ VALIDAZIONE SUPERATA - Configurazione valida")
    
    print()
    print("🎨 Naming personalizzato configurato!")
    
except Exception as e:
    print(f"❌ ERRORE: {str(e)}")
    import traceback
    traceback.print_exc()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # 📋 Riferimento Completo Parametri Scenario
# 
# ## Parametri Principali
# 
# | **Parametro JSON** | **Tipo** | **Descrizione** | **Valore Default** | **Esempio** |
# |-------------------|----------|----------------|-------------------|-------------|
# | `elab_id` | string | Identificativo univoco dell'elaborazione | Auto-generato `flood_YYYYMMDD_HHMMSS` | `"flood_analysis_001"` |
# | `event_name` | string | Nome descrittivo dell'evento alluvionale | `null` | `"Alluvione_Tevere_2024"` |
# 
# ## File di Input
# 
# | **Parametro JSON** | **Tipo** | **Descrizione** | **Valore Default** | **Esempio** |
# |-------------------|----------|----------------|-------------------|-------------|
# | `files.vettoriale` | string | Nome file shapefile edifici | Da dataset `configurazione_dati` | `"edifici_roma.shp"` |
# | `files.raster` | string | Nome file raster profondità acqua | Da dataset `configurazione_dati` | `"flood_depth.tif"` |
# 
# ## Parametri Tecnici Core
# 
# | **Parametro JSON** | **Tipo** | **Descrizione** | **Valore Default** | **Esempio** |
# |-------------------|----------|----------------|-------------------|-------------|
# | `HEIGHT_FIELD` | string | Campo altezza edifici nello shapefile | Da dataset `configurazione_parametri` | `"H_UVL"` |
# | `TARGET_EPSG` | string | Sistema di coordinate di destinazione | Da dataset `configurazione_parametri` | `"32632"` |
# | `REPROJECTION_OPTION` | int | Modalità riproiezione (1=vettoriale→raster, 2=raster→vettoriale, 3=entrambi→EPSG) | Da dataset `configurazione_parametri` | `1` |
# | `BUFFER_DISTANCE` | float/string | Distanza buffer in metri (o "auto") | Da dataset `configurazione_parametri` | `2.5` o `"auto"` |
# 
# ## Parametri di Processamento
# 
# | **Parametro JSON** | **Tipo** | **Descrizione** | **Valore Default** | **Esempio** |
# |-------------------|----------|----------------|-------------------|-------------|
# | `min_valid_height` | float | Altezza minima valida edifici (metri) | `3.0` | `0.5` |
# 
# ## Controlli di Output
# 
# | **Parametro JSON** | **Tipo** | **Descrizione** | **Valore Default** | **Esempio** |
# |-------------------|----------|----------------|-------------------|-------------|
# | `enable_logging` | boolean | Attiva logging dettagliato | `true` | `false` |
# | `create_report` | boolean | Genera report statistico HTML | `true` | `false` |
# | `create_shapefile` | boolean | Salva risultati come shapefile | `true` | `false` |
# 
# ## Naming Personalizzato (Opzionale)
# 
# | **Parametro JSON** | **Tipo** | **Descrizione** | **Valore Default** | **Esempio** |
# |-------------------|----------|----------------|-------------------|-------------|
# | `output_naming.dataset_name` | string | Nome personalizzato dataset output | `null` (usa `"output_inondazioni"`) | `"risultati_tevere_2024"` |
# | `output_naming.folder_name` | string | Nome personalizzato folder output | `null` (usa `"output_inondazioni"`) | `"analisi_tevere"` |
# | `output_naming.file_prefix` | string | Prefisso per file generati | `null` | `"tevere_flood_"` |
# | `output_naming.file_suffix` | string | Suffisso per file generati | `null` | `"_final"` |
# | `output_naming.include_timestamp` | boolean | Include timestamp nei nomi | `true` | `false` |
# 
# ---
# 
# ## 🔧 Note sui Valori Default
# 
# ### **Gerarchia di Priorità**:
# 1. **Parametri JSON Scenario** (priorità massima)
# 2. **Dataset Dataiku** (`configurazione_parametri`, `configurazione_dati`)
# 3. **Valori hardcoded** (priorità minima)
# 
# ### **Parametri Obbligatori**:
# - `HEIGHT_FIELD` - deve essere sempre specificato
# - `TARGET_EPSG` - deve essere sempre specificato  
# - `REPROJECTION_OPTION` - deve essere 1, 2 o 3
# - `files.vettoriale` e `files.raster` - devono esistere
# 
# ### **Valori Speciali**:
# - `BUFFER_DISTANCE: "auto"` - usa risoluzione spaziale del raster
# - `REPROJECTION_OPTION: 1` - riproietta vettoriale nel CRS del raster
# - `REPROJECTION_OPTION: 2` - riproietta raster nel CRS del vettoriale
# - `REPROJECTION_OPTION: 3` - riproietta entrambi nel CRS specificato da TARGET_EPSG
# 
# ---
# 
# ## 📋 Esempio JSON Completo
# 
# ```json
# {
#   "elab_id": "flood_analysis_complete_001",
#   "event_name": "Alluvione Tevere Ottobre 2024",
#   "files": {
#     "vettoriale": "edifici_roma.shp",
#     "raster": "flood_depth_tevere.tif"
#   },
#   "HEIGHT_FIELD": "H_UVL",
#   "TARGET_EPSG": "32632",
#   "REPROJECTION_OPTION": 1,
#   "BUFFER_DISTANCE": "auto",
#   "min_valid_height": 0.5,
#   "enable_logging": true,
#   "create_report": true,
#   "create_shapefile": false,
#   "output_naming": {
#     "dataset_name": "risultati_alluvione_tevere",
#     "folder_name": "output_tevere_analisi",
#     "file_prefix": "tevere_",
#     "file_suffix": "final",
#     "include_timestamp": true
#   }
# }
# ```