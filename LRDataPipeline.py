# -*- coding: utf-8 -*-
"""
Created on Tue Jul 01 22:22 2025

@author: motthomasn@gmail.com

Revision History

Version     Date            Description
0.0         2025-07-01      Initial attempt loosly based around FileIndexer.py
0.1         2025-07-07      Changed to batch processing of data also
                            Added parquet db schema enforcement functions
                            Added further debug level output
                            ToDo: Think about what we want to happen during batch processing in the event of errors.
                            Errors: Check log output from 2025-07-17 and Jira CBR250RRi->Database management subtasks
0.2         2025-07-17      Changed int64 to Int64 in type_mapping dicts to avoid dtype conversion errord. Ref CBR250RRI-27
0.3         2025-07-17      Added casting of both int64 & Int64 to BIGINT but only BIGINT to Int64 (nullable integer). Ref CBR250RRI-27
                            Added logging of script name for tracking of versions
                            Dropped partition columns after loading data from parquet. Ref CBR250RRI-29
0.4         2025-07-17      Added conversion from SQL data type strings to pyarrow data tyype objects. Ref CBR250RRI-29 
                            Also changed pandas type "object" to "string" in type_mapping dicts
0.5         2025-07-17      Passed back boolean status from write_data_to_db() to allow error count to work correctly. Ref CBR250RRI-33 
                            Mirrored method for getting cal_level from cal_name from LifeView class for parquet files as float value not properly stored in previous parquet db. Ref CBR250RRI-32
0.6         2025-07-17      Modified database connection handling to allow transactions to be committed after each batch. Ref CBR250RRI-28
                            Moved parquet database write to separate function.
0.7         2025-07-17      Removed db connection from build_db() as it is not used at this stage and may be causing issues with batch commitment. Ref CBR250RRI-28
                            Changed error handling in write_parquet_data    
0.8         2025-07-17      Committed database tables before writing to parquet. Ref CBR250RRI-34
                            Idea: Pass errored partitions back up the chain and insert error into files table. Then on next try check not only for file presence in table but also error status?            

Class used to maintain CBR250RRi database 
"""

import os
import duckdb
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import sys
import logging
from dataclasses import dataclass
from typing import List, Dict, Generator
from datetime import datetime
from LifeView import LV

@dataclass
class LRData:
    """Container for LRD file metadata and timeseries data"""
    file_name: str
    start_time: datetime
    duration: int
    cal_name: str
    hw_level: int
    cal_level: float
    sw_level: str
    cfg_name: str
    comment: str
    data: pd.DataFrame

class LRDataPipeline:
    """Main class for building and managing the database"""
    
    def __init__(self, config: dict):
        self.db_path = config["db_file"]
        self.time_data_path = config["parquet_dir"]

        self.setup_logging(config["log_level"])
        self.denumDict = config["denumeration_dict"]

        with open("Unwanted_Channels.txt", "r") as f:
            self.unwantedChans = f.read().split("\n")

        self.data_batch_dict = {} # init

         # Define all extensions that the pipeline can actually handle
        self.supported_extensions = {'.LRD', '.lrd', '.SD', '.sd', '.csv', '.CSV', '.parquet'}

        self.setup_database()
    
    def setup_logging(self, log_level: str = 'INFO'):
        """Configure logging with specified level"""
        # Convert string to logging level
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)
        
        # Clear any existing handlers to avoid duplicates
        logging.getLogger().handlers.clear()
        
        logging.basicConfig(
            level=numeric_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('LRDataPipeline.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"CBR250RRi data pipeline begin: {Path(__file__).name}")
        self.logger.info(f"Logging level set to: {log_level.upper()}")

    
    def setup_database(self):
        """Initialize database schema"""
        schema_sql = """        
        -- files table
        CREATE TABLE IF NOT EXISTS files (
            id BIGINT PRIMARY KEY,
            file_name VARCHAR NOT NULL,
            start_time TIMESTAMP,
            duration BIGINT,
            cal_name VARCHAR,
            hw_level INT,
            cal_level DOUBLE,
            sw_level VARCHAR,
            cfg_name VARCHAR,
            comment VARCHAR,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processing_status VARCHAR DEFAULT 'pending',
            error_message VARCHAR
        );

        -- channels table
        CREATE TABLE IF NOT EXISTS channels (
            id BIGINT PRIMARY KEY,
            channel_name VARCHAR NOT NULL,
            data_type VARCHAR NOT NULL,
            description VARCHAR,
            unit VARCHAR
        );
        """
        
        try:
            with duckdb.connect(self.db_path) as conn:
                conn.execute(schema_sql)

            self.logger.info(f"Database initialized: {self.db_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise

    
    def build_db(self, scan_paths: List[str], batch_size: int = 100) -> Dict[str, int]:
        """
        Main method to build the database
        Idea! We should scan directories here as normal but with different actions depending on if the files are parquet or LRD
        Batch sizes are only valid for LRD files. After cycling a parquet file, the current batch is processed
        
        """
        self._stats = {'processed': 0, 'skipped': 0, 'errors': 0, 'total': 0}
        
        self.logger.info("Starting data ingestion process")

        try:
            # Process files in batches
            batch = []
            
            for file_path in self.scan_directories(scan_paths):
                self._stats['total'] += 1
                
                batch.append(file_path)
                
                # Process batch when full or ig the passed is parquet
                if (len(batch) >= batch_size) | ( file_path.suffix == ".parquet" ):
                    # single parquet file likely to already contain several LRD files so just go ahead & process it as a batch
                    batch_stats = self.process_batch(batch)
                    self._stats['processed'] += batch_stats['processed']
                    self._stats['errors'] += batch_stats['errors']
                    # Note: 'skipped' now happens inside process_single_file() and counts as 'processed'
                    batch = []
                    self._current_batch = []  # Clear after successful processing
                    
                    self.logger.info(f"Progress: {self._stats['processed']} processed, "
                                   f"{self._stats['errors']} errors")
            
            
            # once complete, the parquet files schema must be commonised
            self.enforce_schema()

            self.logger.info("  Data ingestion complete:")
            
            self.logger.info(f"  Total files found: {self._stats['total']}")
            self.logger.info(f"  Files processed: {self._stats['processed']}")
            self.logger.info(f"  Files with errors: {self._stats['errors']}")
                
        except KeyboardInterrupt:
            # This shouldn't happen due to signal handler, but just in case
            self.logger.warning("KeyboardInterrupt caught in build_index")
            if batch:
                self.logger.info("Processing current batch before exit...")
                try:
                    batch_stats = self.process_batch(batch)
                    self._stats['processed'] += batch_stats['processed']
                    self._stats['errors'] += batch_stats['errors']
                    self.logger.info(f"Emergency batch save: {batch_stats['processed']} processed, {batch_stats['errors']} errors")
                except Exception as save_error:
                    self.logger.error(f"Failed to save batch on KeyboardInterrupt: {save_error}")

        except Exception as e:
            self.logger.error(f"Unexpected error in build_index: {e}")
            # Try to save current batch even on unexpected errors
            if batch:
                try:
                    self.logger.info("Attempting to save current batch due to error...")
                    batch_stats = self.process_batch(batch)
                    self._stats['processed'] += batch_stats['processed']
                    self._stats['errors'] += batch_stats['errors']
                except Exception as save_error:
                    self.logger.error(f"Failed to save batch on error: {save_error}")
            raise

        finally:
            self._current_batch = []
            
        return self._stats


    def enforce_schema(self):
        """
        Function ensures schema commonality across parquet database after updating
        channels table in database can be used as master
        Need to be careful to preserve column data types when adding pad columns 
        """
        sql_to_pa_type = {
                        "BIGINT": pa.int64(),
                        "DOUBLE": pa.float64(),
                        "VARCHAR": pa.string(),
                        "BOOLEAN": pa.bool_(),
                        "TIMESTAMP": pa.timestamp("ns")
                        }
        
        # first get master schema from channels table
        with duckdb.connect(self.db_path) as conn:
            ms = conn.execute("""
                            SELECT channel_name, data_type 
                            FROM channels 
                            """).fetchall()
        
        master_schema = {chan:dtype for (chan,dtype) in ms}
        
        # create dict and then set
        master_cols = set(master_schema.keys())
        
        # now go through database
        for parquet_path in self.time_data_path.rglob("*.parquet"):
            try:
                self.logger.debug(f"Reading schema from {parquet_path.name}")
                pq_file = pq.ParquetFile(parquet_path)
                existing_schema = pq_file.schema_arrow

                # Build new schema
                existing_cols = set(existing_schema.names)

                # Determine missing columns
                missing_cols = master_cols - existing_cols

                if missing_cols:
                    self.logger.debug(f"Updating schema for {parquet_path.name}")
                    # Read file into table (lightweight)
                    table = pq.read_table(parquet_path)

                    # Add missing columns with null values and correct type
                    for col in missing_cols:
                        col_type = sql_to_pa_type[master_schema[col]]
                        table = table.append_column(col, pa.array([None] * table.num_rows, type=col_type))

                    # Reorder columns to match master schema
                    reordered = table.select(list(master_schema.keys()))

                    # Write back to db
                    pq.write_table(reordered, parquet_path)
                    self.logger.debug(f"Updated table written back to {parquet_path}")
            
            except Exception as e:
                self.logger.error(f"Error updating schema for {parquet_path}: {e}")


    def enforce_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to enforce column data types in pandas dataframe prior to writing to parquet
        Similar functionality to 
        """

        type_mapping = { # inverse of dict in write_data_to_db()
                        "BIGINT":"Int64",
                        "DOUBLE":"float64",
                        "VARCHAR":"string",
                        "BOOLEAN":"bool",
                        "TIMESTAMP":"datetime64[ns]"
                        }
        
        try:
            # first get master schema from channels table
            with duckdb.connect(self.db_path) as conn:
                ms = conn.execute("""
                                SELECT channel_name, data_type 
                                FROM channels 
                                """).fetchall()
            
            master_schema = {chan:type_mapping[dtype] for (chan,dtype) in ms}

            for col in df.columns:
                df[col] = df[col].astype(master_schema[col])

            return df
        
        except Exception as e:
            self.logger.error(f"Error enforcing dtypes to dataframe: {e}")
            return df


    def scan_directories(self, root_paths: List[Path]) -> Generator[Path, None, None]:
        """Scan directories for supported files"""
        total_files = 0
        
        for root in root_paths:
            # root = Path(root_path)
            if not root.exists():
                self.logger.warning(f"Directory does not exist: {str(root)}")
                continue
                
            self.logger.info(f"Scanning directory: {str(root)}")
            
            try:
                for file_path in root.rglob('*'):
                    if (file_path.is_file() and 
                        file_path.suffix in self.supported_extensions):
                        
                        try:
                            total_files += 1
                            yield file_path
                            
                        except (OSError, PermissionError) as e:
                            self.logger.warning(f"Cannot access {file_path}: {e}")
                            
            except Exception as e:
                self.logger.error(f"Error scanning {str(root)}: {e}")
        
        self.logger.info(f"Found {total_files} files to process")


    def process_batch(self, batch: List[Path]) -> Dict[str, int]:
        """Process a batch of files in a transaction"""
        stats = {'processed': 0, 'errors': 0}
        
        with duckdb.connect(self.db_path) as conn:
            conn.execute("BEGIN TRANSACTION")
            self.logger.debug("Batch processing started...")
            try:
                for file_path in batch:
                    if file_path.suffix==".parquet":
                        # process parquet
                        if self.process_parquet_file(conn, file_path):
                            stats['processed'] += 1
                        else:
                            stats['errors'] += 1

                    elif self.process_LRD_file(conn, file_path):
                        stats['processed'] += 1
                    else:
                        stats['errors'] += 1
                
                conn.execute("COMMIT")

                # Write data dict to parquet db before committing tables
                self.write_parquet_data()
                
                self.logger.debug("Batch data committed to database")

            except Exception as e:
                conn.execute("ROLLBACK")
                self.logger.error(f"Batch processing failed: {e}")
                stats['errors'] += len(batch)

        return stats


    def write_parquet_data(self):
        """
        Write accumulated data to parquet files
        """
        failed_partitions = {}

        while self.data_batch_dict:
            try:
                # using popitem() ensures we remove the items from the dict as we iterate so if there is an error raised we can keep the dict
                partition_file, data = self.data_batch_dict.popitem()

                partition_file.parent.mkdir(parents=True, exist_ok=True)
                
                if partition_file.is_file():
                    # data already exists so read and concat
                    existing_df = pq.read_table(partition_file).to_pandas().drop(columns=["year", "month"]) # need to remove partition columns before appending
                    data_to_write = pd.concat([existing_df, data], ignore_index=True)
                else:
                    # create the partition
                    data_to_write = data

                # before writing, enforce data types to protect against possible upcasting that may have happened during concat operations
                typed_data = self.enforce_dtypes(data_to_write)

                # Write entire updated partition
                table = pa.Table.from_pandas(typed_data, preserve_index=False)
                pq.write_table(table, partition_file, compression='snappy', coerce_timestamps='us', allow_truncated_timestamps=True)
                self.logger.debug(f"Parquet partition written successfully to {partition_file}")

            except Exception as e:
                # put the data back into the dict
                failed_partitions[partition_file] = data
                self.logger.error(f"Reading or writing parquet partition {partition_file}: {e}")
                break

        # Put failed partitions back for potential retry
        self.data_batch_dict.update(failed_partitions)
        
        if failed_partitions:
            raise Exception(f"Failed to write {len(failed_partitions)} parquet partitions")


    def process_parquet_file(self, conn: duckdb.DuckDBPyConnection, file_path: Path) -> bool:
        """Process a single parquet file and process each contained LRD file separately"""
        
        try:
            self.logger.debug(f"Processing parquet file at {file_path}")
            # read the file as pandas dataframe and remove any columns where values are all NaN
            df = pd.read_parquet(file_path)
            df.dropna(axis=1, how="all", inplace=True)

            # remove unwanted channels here
            df.drop(columns=self.unwantedChans, inplace=True, errors='ignore')

            # get & add partition info
            for part in file_path.parts:
                if "=" in part:
                    (partName, partVal) = part.split("=")
                    if partName=="hwLevel":
                        partVal = int(partVal)
                    elif partName=="calLevel":
                        partVal = float(partVal)
                    df[partName] = partVal

            # group by file name
            by_fname = df.groupby("fileName")

            # loop through individual file data
            for filename in by_fname.groups:
                grpDf = by_fname.get_group(filename).dropna(axis=1, how="all").copy()
                # as we are partitioning by different values, remove all NaN columns from the grouped df also

                # check if file already processed
                cursor = conn.execute("""
                    SELECT id, file_name, processing_status 
                    FROM files 
                    WHERE file_name = ?
                """, (filename,))
                existing_record = cursor.fetchone()

                if not existing_record:
                    # if not, create dataclass instance and delete meta-data from dataframe
                    lrd_obj = LRData(
                                    file_name=filename,
                                    start_time=grpDf["timeStamp"].min(),
                                    duration=int(( grpDf["timeStamp"].max() - grpDf["timeStamp"].min() ).total_seconds()),
                                    cal_name=grpDf["calName"].unique()[0],
                                    hw_level=int(grpDf["hwLevel"].unique()[0]),
                                    cal_level=float(grpDf["calName"].unique()[0].split("_")[2].split("-")[1]),
                                    sw_level=grpDf["swLevel"].unique()[0],
                                    cfg_name=grpDf["cfgName"].unique()[0],
                                    comment=grpDf["fileComment"].unique()[0],
                                    data=grpDf.drop(columns=["fileName", "calName", "hwLevel", "calLevel", "swLevel", "cfgName", "fileComment"])
                                    )
                    
                    self.logger.debug(f"Retrieved data for LRD file {filename} from parquet file at {file_path}")
                    if not self.write_data_to_db(conn, file_path, lrd_obj):
                        return False

                else:
                    self.logger.debug(f"{filename} record already exists in database")

            return True
        
        except Exception as e:
            self.logger.error(f"Failed to load or process {file_path.name} with {filename}: {e}")
            return False


    def write_data_to_db(self, conn: duckdb.DuckDBPyConnection, file_path: Path, lrd_data: LRData) -> bool:
        """Write the LRD data to database. Regardless of source, the process is the same once the LRData class has been populated"""
                    
        try:
            new_file_id = conn.execute("""SELECT COALESCE(MAX(id), 0) + 1 
                                    AS next_id 
                                    FROM files;""").fetchone()[0] # fetchone() still returns a tuple even if there is only one value

            lrd_data.data["file_id"] = new_file_id

            # insert file matadata to files table
            conn.execute("""
                        INSERT INTO files 
                        (id, file_name, start_time, duration, cal_name, hw_level, cal_level, sw_level, cfg_name, comment, processing_status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'processing');
                        """, (new_file_id, lrd_data.file_name, lrd_data.start_time, 
                        lrd_data.duration, lrd_data.cal_name, lrd_data.hw_level,
                        lrd_data.cal_level, lrd_data.sw_level, lrd_data.cfg_name, lrd_data.comment))
            
            self.logger.debug(f"Inserted new file record with ID {new_file_id}: {lrd_data.file_name}")

            # insert any new columns to channels table
            existing_channels = conn.execute("""
                                            SELECT channel_name 
                                            FROM channels
                                            """).fetchall() # what form is returned? List (or empty list) of tuples 
            existing_channels = [chan for chan, in existing_channels] # must flatten to list of string items or set operation will not work as expected
            
            next_chan_id = conn.execute("""SELECT COALESCE(MAX(id), 0) + 1 
                                    AS next_chan_id 
                                    FROM channels;""").fetchone()[0]

            new_channels = list(set(lrd_data.data.columns).difference(existing_channels)) # this needs adjusting once we know what form

            if new_channels:
                # insert new channels to channels table with datatype
                # channel_info = [(id, name, dtype), ..]
                type_mapping = { # inverse of dict in enforce_dtypes()
                                "int64": "BIGINT",
                                "Int64": "BIGINT",
                                "float64": "DOUBLE",
                                "string": "VARCHAR",
                                "bool": "BOOLEAN",
                                "datetime64[ns]": "TIMESTAMP"
                                }

                channel_info = [
                                (next_chan_id+i, name, type_mapping[str(lrd_data.data[name].dtype)])
                                for i,name in enumerate(new_channels)
                                ]
        
                conn.executemany("""
                                INSERT INTO channels (id, channel_name, data_type)
                                VALUES (?, ?, ?)
                                """, channel_info)
                
                self.logger.debug(f"Inserted new channel records from new file ID {new_file_id}: {lrd_data.file_name} into channels table")
            
            # Add data to partitioned parquet database
            # create partition path manually, check if it exists. If it does, load the existing parquet append the data to it and re-save
            year, month = lrd_data.start_time.year, lrd_data.start_time.month
            
            partition_file = self.time_data_path / f"year={year}" / f"month={month}" / f"{year}{month:02}_data.parquet"

            if partition_file in self.data_batch_dict:
                _temp = self.data_batch_dict[partition_file]
                self.data_batch_dict[partition_file] = pd.concat([_temp, lrd_data.data], ignore_index=True)
            else:
                self.data_batch_dict[partition_file] = lrd_data.data
            
            self.logger.debug(f"{lrd_data.file_name} time data added to partition dictionary for writing to db")

            # Mark as completed
            conn.execute("""
                UPDATE files SET processing_status = 'completed', error_message = NULL
                WHERE id = ?
            """, (new_file_id,))
        
            self.logger.debug(f"Processed new file {lrd_data.file_name} from {file_path.name}: {lrd_data.duration}s log duration")

            return True
            
        except Exception as e:
            error_msg = str(e)[:500]  # Limit error message length
            conn.execute("""
                UPDATE files SET processing_status = 'error', error_message = ?
                WHERE id = ?
            """, (error_msg, new_file_id))
            self.logger.error(f"Failed to process {file_path.name}: {e}")
            return False


    def process_LRD_file(self, conn: duckdb.DuckDBPyConnection, file_path: Path) -> bool:
        """Process a single LRD file and extract its metadata"""
        try:
            
            # Check if this file already exists in the database
            cursor = conn.execute("""
                SELECT id, file_name, processing_status 
                FROM files 
                WHERE file_name = ?
            """, (file_path.name,))
            existing_record = cursor.fetchone()
            
            if not existing_record:            
                # No existing record - this is a genuinely new file, proceed with full processing
                self.logger.debug(f"Processing new LRD file: {file_path}")

                lrdObj = LV(file_path, 
                            export=False, 
                            read_data=True, 
                            sampleRate=100, 
                            dictFile=self.denumDict)
                
                # copy relevant attributes into the data class
                lrd_obj = LRData(
                                file_name=lrdObj.fileName,
                                start_time=lrdObj.startTime,
                                duration=int(( lrdObj.data["timeStamp"].max() - lrdObj.data["timeStamp"].min() ).total_seconds()),
                                cal_name=lrdObj.calName,
                                hw_level=lrdObj.hwLevel,
                                cal_level=lrdObj.calLevel,
                                sw_level=lrdObj.swLevel,
                                cfg_name=lrdObj.cfgName,
                                comment=lrdObj.comment,
                                data=lrdObj.data
                                )
                
                lrdObj.data.dropna(axis=1, how="all", inplace=True) # drop columns with all NaN values
                lrdObj.data.drop(columns=self.unwantedChans, errors='ignore') # drop unwanted channels
                
                self.logger.debug(f"Retrieved data for LRD file {file_path.name}")
                if not self.write_data_to_db(conn, file_path, lrd_obj):
                    return False

            else:
                self.logger.debug(f"{file_path.name} already exists in database")
                    
            return True

        except Exception as e:
            self.logger.error(f"Failed to load {file_path.name}: {e}")
            return False
        

def load_config(config_file: str = 'config.yml') -> Dict:
    """Load configuration from YAML file and resolve paths"""
    try:
        import yaml
        if Path(config_file).is_file():
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f) or {}

            # get google drive base path
            gdrive = Path(os.environ["GoogleDrive"])

            # make absolute paths
            config["data_dir"] = [gdrive.joinpath(relPth) for relPth in config["data_rel_path"]]
            config["parquet_dir"] = gdrive.joinpath(config["db_rel_path"], "time_data")
            config["translation_dict"] = gdrive.joinpath(config["denumeration_dict"])
            config["db_file"] = gdrive.joinpath(config["db_rel_path"], f'{config["db"]}.duckdb')
            config["meta_table"] = "files"

            return config
        else:
            return {}
    except ImportError:
        print("Warning: PyYAML not available. Skipping config file.")
        return {}
    except Exception as e:
        print(f"Warning: Could not load config file {config_file}: {e}")
        return {}
    

def main():
    # Load configuration from file
    cfg = load_config("config.yml")
    
    print("Configuration loaded from: config.yml")
    print(f"Directories to scan: {cfg['data_dir']}")
    print(f"Database: {cfg['db']}")
    print(f"Batch size: {cfg['batch_size']}")
    print(f"Log level: {cfg['log_level']}")
    
    # Create pipeline
    pipeline = LRDataPipeline(cfg)
    
    # Build database
    stats = pipeline.build_db(cfg['data_dir'], cfg['batch_size'])
    
    print("Ingestion Complete!")
    print(f"Total files found: {stats['total']}")
    print(f"Files processed: {stats['processed']}")
    print(f"Files skipped (unchanged): {stats['skipped']}")
    print(f"Files with errors: {stats['errors']}")


if __name__ == "__main__":
    main()
