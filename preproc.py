import reverse_geocoder as rg
import shapefile

from collections import OrderedDict
import os

# define paths
ROOT_DIR = os.getcwd()
FIRE_SHP_FP = f"{ROOT_DIR}/data/fire_shapefiles"
FIRE_DIR = f"{ROOT_DIR}/data/fire"
FIRE_DATASET_FP = f"{FIRE_DIR}/fire_dataset.csv"
CA_FIRE_DATASET_FP = f"{FIRE_DIR}/ca_fire_dataset.csv"
FIRE_DATASET_HEADER = [ "DATE", "GMT", "LAT", "LON", "NAME", "ADMIN1", "ADMIN2", "AREA", "TEMP"]

def parse_shp_to_csv():
	"""
	Open up shapefiles > reverse geo code lat lons to get city/county names > write to csv
	"""
	with open(FIRE_DATASET_FP, "w") as out:
		out.write(",",join(FIRE_DATASET_HEADER))

	row_count = 0 
	for year_dir in os.listdir(FIRE_SHP_FP):
		# read shapefile
		fn = year_dir.replace("_shapefile", ".shp")
		fp = f"{FIRE_SHP_FP}/{year_dir}/{fn}"
		sf = shapefile.Reader(fp)
		fields = sf.fields[1:]
		columns = [f[0] for f in fields]
		records = sf.records()
		# reverse geocode lat lon to retrieve location names
		lat_lons = [(record[columns.index("LAT")], record[columns.index("LONG")]) for record in records]
		results = rg.search(lat_lons)
		data = []
		for i, result in enumerate(results):
			# add rows if US
			if result['cc'].lower() == 'us':
				row = OrderedDict()
				row["DATE"]   = str(records[i][ columns.index('DATE')])
				row["GMT"]    = str(records[i][ columns.index('GMT')])
				row["LAT"]    = str(records[i][columns.index("LAT")])
				row["LON"]    = str(records[i][columns.index("LONG")])
				row["NAME"]   = str(result["name"])
				row["ADMIN1"] = str(result["admin1"])
				row["ADMIN2"] = str(result["admin2"])
				row["AREA"]   = str(records[i][ columns.index('AREA')])
				row["TEMP"]   = str(records[i][ columns.index('TEMP')])
				data.append(row)
				# write data out
				if len(data) == 100000:
					row_count += len(data)
					print(f"Processing {fn}, row count: {row_count}")
					with open(dataset_fn, "a+") as out:
						for row in data:
							out.write(','.join(list(row.values())) + "\n")
					data = []
		# write remaining data out
		if data:
			row_count += len(data)
			print(f"Processing {FIRE_DATASET_FP}, row count: {row_count}")
			with open(FIRE_DATASET_FP, "a+") as out:
				for row in data:
					out.write(','.join(list(row.values())) + "\n")


def filter_dataset_to_ca():
	fire_df = pd.read_csv(FIRE_DATASET_FP, names=FIRE_DATASET_HEADER)
	ca_fire_df = fire_df[fire_df["ADMIN1"] == "California"] # ADMIN1 is state name, will always be written like 'California'
	df.to_csv(CA_FIRE_DATASET_FP, index=False)


def main():
	parse_shp_to_csv()
	filter_dataset_to_ca()


if __name__ == "__main__":
	main()
	
