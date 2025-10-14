"""
ANALISI SOMMERSIONE EDIFICI
Calcola la percentuale di sommersione degli edifici durante eventi alluvionali
analizzando la profondità dell'acqua nei pixel esterni al perimetro di ciascun edificio.

Input: shapefile edifici + raster profondità acqua
Output: shapefile con percentuali sommersione + report statistico
Metodo: campionamento pixels esterni (buffer 1 pixel)
"""

import geopandas as gpd
import rasterio
import rasterio.mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
from shapely.geometry import mapping
from shapely.ops import unary_union
import fiona
import os
import sys
import tempfile
import logging
from datetime import datetime

# Percorsi input/output
VECTOR_PATH = r"E:\RECOVERY\WORK\IN-TIME\FLOODING\WATER-DEPTH\GORO_V_UVL_GPG.shp"
RASTER_PATH = r"E:\RECOVERY\WORK\IN-TIME\FLOODING\WATER-DEPTH\emilia_extract_02_depth_with_nodata.tif"
OUTPUT_PATH = r"E:\RECOVERY\WORK\IN-TIME\FLOODING\WATER-DEPTH\wd_analysis\wd_estimation.shp"

# Parametri configurazione
HEIGHT_FIELD = "H_UVL"  # Nome del campo altezza edificio nel vettoriale di input
REPROJECTION_OPTION = 1  # 1=riproietta vettoriale, 2=riproietta raster, 3=riproietta entrambi
TARGET_EPSG = "32632"    # EPSG di destinazione (usato solo se REPROJECTION_OPTION = 3)
BUFFER_DISTANCE = None   # Distanza buffer in metri (None = automatico = risoluzione pixel)

# Configura logging
log_path = OUTPUT_PATH.replace('wd_estimation.shp', 'wd_estimation.log')
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path, mode='w', encoding='utf-8')
    ]
)

# Ridefinisci print per logging
original_print = print
def print(*args, **kwargs):
    message = ' '.join(str(arg) for arg in args)
    logging.info(message)

# Log inizio elaborazione
logging.info("=== INIZIO ELABORAZIONE ===")
logging.info(f"File vettoriale: {VECTOR_PATH}")
logging.info(f"File raster: {RASTER_PATH}")
logging.info(f"File output: {OUTPUT_PATH}")

# Carica dati vettoriali e raster
vector = gpd.read_file(VECTOR_PATH)
raster = rasterio.open(RASTER_PATH)

# Controlla i campi disponibili nel vettoriale
print("Campi disponibili nel vettoriale:")
print(list(vector.columns))
print()

# Controllo CRS
vector_crs = vector.crs
raster_crs = raster.crs

if vector_crs != raster_crs:
    print(f"ATTENZIONE: I sistemi di riferimento non coincidono!")
    print(f"CRS vettoriale: {vector_crs}")
    print(f"CRS raster: {raster_crs}")
    print(f"Applicando opzione di riproiezione: {REPROJECTION_OPTION}")
    
    try:
        if REPROJECTION_OPTION == 1:
            # Riproietta vettoriale nel CRS del raster
            target_crs = raster_crs
            print(f"Riproiettando il vettoriale in {target_crs}...")
            vector = vector.to_crs(target_crs)
            print("Vettoriale riproiettato.")
            
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
            print("Raster riproiettato.")
            
        elif REPROJECTION_OPTION == 3:
            # Riproietta entrambi nel CRS specificato
            target_crs = f"EPSG:{TARGET_EPSG}"
            print(f"Riproiettando entrambi in {target_crs}...")
            
            # Riproietta vettoriale
            vector = vector.to_crs(target_crs)
            print("Vettoriale riproiettato.")
            
            # Riproietta raster
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
            print("Raster riproiettato.")
            
        else:
            logging.error(f"Opzione di riproiezione non valida: {REPROJECTION_OPTION}")
            original_print("Elaborazione fallita - Exit code: 1")
            sys.exit(1)
            
    except Exception as e:
        logging.error(f"Errore durante la riproiezione: {e}")
        original_print("Elaborazione fallita - Exit code: 1")
        sys.exit(1)

# Funzione per ottenere i pixel esterni al perimetro
def get_external_pixels(geom, raster, buffer_distance=None):
    """
    Estrae i valori dei pixel immediatamente esterni al perimetro del poligono
    """
    try:
        # Se non specificato, usa la risoluzione del raster come buffer
        if buffer_distance is None:
            buffer_distance = abs(raster.transform[0])  # risoluzione pixel
        
        # Crea buffer esterno molto piccolo
        external_buffer = geom.buffer(buffer_distance)
        
        # Crea anello: buffer esterno - poligono originale
        ring = external_buffer.difference(geom)
        
        # Estrai valori raster dall'anello
        out_image, out_transform = rasterio.mask.mask(raster, [mapping(ring)], crop=True, filled=True)
        data = out_image[0]
        
        # Escludi nodata
        valid_data = data[data != raster.nodata]
        
        return valid_data
        
    except Exception as e:
        return np.array([])

# Prepara lista risultati e contatori
results = []
processed_count = 0
not_processed_count = 0

print(f"\nElaborazione di {len(vector)} edifici...")

for idx, row in vector.iterrows():
    geom = row.geometry
    a_base = geom.area  # Calcola area dalla geometria
    h_uvl = row[HEIGHT_FIELD]  # Legge altezza dal campo parametrizzato
    vol = a_base * h_uvl
    
    # Estrai valori esterni al perimetro
    external_values = get_external_pixels(geom, raster, BUFFER_DISTANCE)
    
    if external_values.size > 0 and h_uvl > 0:
        # Calcola statistiche di sommersione
        depth_mean = np.mean(external_values)
        depth_min = np.min(external_values)
        depth_max = np.max(external_values)
        
        # Calcola percentuale di sommersione basata sulla quota media
        perc_submerged = min((depth_mean / h_uvl) * 100, 100.0)
        
        processed_count += 1
        
    else:
        # Nessun dato valido o edificio con altezza zero
        depth_mean = 0.0
        depth_min = 0.0
        depth_max = 0.0
        perc_submerged = 0.0
        not_processed_count += 1
    
    results.append({
        'A_BASE': round(a_base, 2),
        HEIGHT_FIELD: round(h_uvl, 2),
        'VOL': round(vol, 2),
        'DEPTH_MEAN': round(depth_mean, 2),
        'DEPTH_MIN': round(depth_min, 2),
        'DEPTH_MAX': round(depth_max, 2),
        'PERC_SUBM': round(perc_submerged, 2),
        'geometry': geom
    })
    
    # Progress indicator
    if (idx + 1) % 100 == 0:
        print(f"Elaborati {idx + 1}/{len(vector)} edifici...")

# Crea GeoDataFrame di output
out_gdf = gpd.GeoDataFrame(results, crs=vector.crs)

# Crea cartella output se non esiste
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# Definisci schema per campi con precisione limitata
schema = {
    'geometry': 'Polygon',
    'properties': {
        'A_BASE': 'float:10.2',      # 10 cifre totali, 2 decimali
        HEIGHT_FIELD: 'float:8.2',   # 8 cifre totali, 2 decimali
        'VOL': 'float:12.2',         # 12 cifre totali, 2 decimali
        'DEPTH_MEAN': 'float:8.2',   # 8 cifre totali, 2 decimali
        'DEPTH_MIN': 'float:8.2',    # 8 cifre totali, 2 decimali
        'DEPTH_MAX': 'float:8.2',    # 8 cifre totali, 2 decimali
        'PERC_SUBM': 'float:6.2'     # 6 cifre totali, 2 decimali
    }
}

# Salva shapefile con schema definito
with fiona.open(OUTPUT_PATH, 'w', driver='ESRI Shapefile', crs=out_gdf.crs, schema=schema) as f:
    for idx, row in out_gdf.iterrows():
        feature = {
            'geometry': mapping(row.geometry),
            'properties': {
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

# Riepilogo finale
total_buildings = len(vector)
print(f"\n=== RIEPILOGO ELABORAZIONE ===")
print(f"Edifici totali: {total_buildings}")
print(f"Elaborati con successo: {processed_count}")
print(f"Non processati: {not_processed_count}")
print(f"Output scritto in: {OUTPUT_PATH}")

# Calcola statistiche per il report
if processed_count > 0:
    processed_data = out_gdf[out_gdf['DEPTH_MEAN'] > 0]
    
    if len(processed_data) > 0:
        # Statistiche sui livelli di sommersione
        mean_depth_avg = processed_data['DEPTH_MEAN'].mean()
        max_depth_avg = processed_data['DEPTH_MEAN'].max()
        min_depth_avg = processed_data['DEPTH_MEAN'].min()
        
        mean_depth_max = processed_data['DEPTH_MAX'].mean()
        max_depth_max = processed_data['DEPTH_MAX'].max()
        min_depth_max = processed_data['DEPTH_MAX'].min()
        
        # Statistiche percentuali sommersione
        mean_perc_subm = processed_data['PERC_SUBM'].mean()
        max_perc_subm = processed_data['PERC_SUBM'].max()
        min_perc_subm = processed_data['PERC_SUBM'].min()
        median_perc_subm = processed_data['PERC_SUBM'].median()
        std_perc_subm = processed_data['PERC_SUBM'].std()
        
        # Statistiche edifici
        mean_altezza = processed_data[HEIGHT_FIELD].mean()
        max_altezza = processed_data[HEIGHT_FIELD].max()
        min_altezza = processed_data[HEIGHT_FIELD].min()
        
        mean_area = processed_data['A_BASE'].mean()
        max_area = processed_data['A_BASE'].max()
        min_area = processed_data['A_BASE'].min()
        
        # Classificazione edifici per livello di sommersione
        edifici_bassi = len(processed_data[processed_data['PERC_SUBM'] < 25])
        edifici_medi = len(processed_data[(processed_data['PERC_SUBM'] >= 25) & (processed_data['PERC_SUBM'] < 75)])
        edifici_alti = len(processed_data[(processed_data['PERC_SUBM'] >= 75) & (processed_data['PERC_SUBM'] < 100)])
        edifici_totali = len(processed_data[processed_data['PERC_SUBM'] >= 100])
        
        # Correlazioni e analisi avanzate
        correlazione_altezza_sommersione = processed_data[HEIGHT_FIELD].corr(processed_data['PERC_SUBM'])
        correlazione_area_sommersione = processed_data['A_BASE'].corr(processed_data['PERC_SUBM'])
        
        # Statistiche di variabilità delle profondità
        range_profondita = processed_data['DEPTH_MAX'] - processed_data['DEPTH_MIN']
        variabilita_media = range_profondita.mean()
        variabilita_max = range_profondita.max()
        
        # Percentili di interesse
        perc_25 = processed_data['PERC_SUBM'].quantile(0.25)
        perc_75 = processed_data['PERC_SUBM'].quantile(0.75)
        perc_90 = processed_data['PERC_SUBM'].quantile(0.90)
        perc_95 = processed_data['PERC_SUBM'].quantile(0.95)
        
        # Densità edifici per gravità danneggiamento
        # Calcola area geografica con convex hull degli edifici analizzati
        edifici_geometrie = processed_data.geometry
        convex_hull = unary_union(edifici_geometrie).convex_hull
        superficie_totale_analizzata = convex_hull.area / 10000  # in ettari
        densita_edifici_critici = len(processed_data[processed_data['PERC_SUBM'] >= 50]) / superficie_totale_analizzata if superficie_totale_analizzata > 0 else 0
        
        # Volume teorico acqua nell'area edifici (approssimativo)
        volume_acqua_stimato = (processed_data['DEPTH_MEAN'] * processed_data['A_BASE']).sum()
        
        # Crea report TXT
        report_path = OUTPUT_PATH.replace('.shp', '_report.txt')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=== REPORT ANALISI SOMMERSIONE EDIFICI ===\n\n")
            f.write(f"Data elaborazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Versione script: wd_estimation.py\n")
            f.write(f"Campo altezza utilizzato: {HEIGHT_FIELD}\n")
            f.write(f"Opzione riproiezione: {REPROJECTION_OPTION} ")
            if REPROJECTION_OPTION == 1:
                f.write("(riproietta vettoriale)\n")
            elif REPROJECTION_OPTION == 2:
                f.write("(riproietta raster)\n")
            else:
                f.write(f"(riproietta entrambi in {TARGET_EPSG})\n")
            f.write("\n")
            
            f.write("=== FILE DI INPUT/OUTPUT ===\n")
            f.write(f"File vettoriale: {VECTOR_PATH}\n")
            f.write(f"File raster: {RASTER_PATH}\n")
            f.write(f"File output: {OUTPUT_PATH}\n")
            f.write(f"File report: {report_path}\n")
            f.write(f"File log: {log_path}\n\n")
            
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
            f.write(f"Edifici con sommersione rilevata: {len(processed_data)} ({len(processed_data)/total_buildings*100:.1f}%)\n")
            f.write(f"Edifici non processati: {not_processed_count} ({not_processed_count/total_buildings*100:.1f}%)\n")
            f.write(f"  - Cause: senza sovrapposizione con raster, altezza zero/negativa, errori geometrici\n\n")
            
            f.write("=== METODOLOGIA ===\n")
            f.write("L'analisi calcola la sommersione degli edifici campionando i valori di profondità\n")
            f.write("dell'acqua nei pixel esterni al perimetro di ciascun edificio (buffer di 1 pixel).\n")
            f.write("La percentuale di sommersione è calcolata come: (profondità_media / altezza_edificio) × 100\n")
            f.write("I valori sono limitati al 100% per edifici completamente sommersi.\n\n")
            
            f.write("=== PROFONDITÀ ACQUA ===\n")
            f.write(f"Profondità media: {mean_depth_avg:.2f} m (range: {min_depth_avg:.2f} - {max_depth_avg:.2f} m)\n")
            f.write(f"Profondità massima rilevata: {max_depth_max:.2f} m\n\n")
            
            f.write("=== CARATTERISTICHE TERRITORIO ===\n")
            f.write(f"Area geografica analizzata (convex hull): {superficie_totale_analizzata:.1f} ettari\n")
            f.write(f"Edifici con danno significativo (≥50%): {len(processed_data[processed_data['PERC_SUBM'] >= 50])} su {len(processed_data)}\n")
            f.write(f"Densità edifici critici: {densita_edifici_critici:.1f} edifici/ettaro\n")
            f.write(f"Altezza media edifici: {mean_altezza:.1f} m (range: {min_altezza:.1f} - {max_altezza:.1f} m)\n\n")
            
            f.write("=== CLASSIFICAZIONE EDIFICI PER LIVELLO SOMMERSIONE ===\n")
            f.write(f"Sommersione bassa (<25%): {edifici_bassi} edifici ({edifici_bassi/len(processed_data)*100:.1f}%)\n")
            f.write(f"Sommersione media (25-75%): {edifici_medi} edifici ({edifici_medi/len(processed_data)*100:.1f}%)\n")
            f.write(f"Sommersione alta (75-99%): {edifici_alti} edifici ({edifici_alti/len(processed_data)*100:.1f}%)\n")
            f.write(f"Completamente sommersi (≥100%): {edifici_totali} edifici ({edifici_totali/len(processed_data)*100:.1f}%)\n\n")
            
            f.write("=== CAMPI OUTPUT SHAPEFILE ===\n")
            f.write("DEPTH_AVG: Profondità media dell'acqua attorno all'edificio (m)\n")
            f.write("DEPTH_MAX: Profondità massima dell'acqua attorno all'edificio (m)\n")
            f.write("DEPTH_MIN: Profondità minima dell'acqua attorno all'edificio (m)\n")
            f.write("PERC_SUBM: Percentuale di sommersione dell'edificio (%)\n")
            f.write(f"{HEIGHT_FIELD}: Altezza dell'edificio utilizzata nel calcolo (m)\n")
            f.write("AREA_BASE: Area della base dell'edificio (m²)\n")
            f.write("+ tutti i campi originali del vettoriale di input\n")
            
        print(f"Report statistico scritto in: {report_path}")

# Pulizia file temporaneo se creato
if 'temp_raster_path' in locals():
    raster.close()
    os.unlink(temp_raster_path)

# Exit code finale
logging.info("Elaborazione completata con successo")
original_print("Elaborazione completata con successo - Exit code: 0")
original_print(f"Log completo disponibile in: {log_path}")
sys.exit(0)
