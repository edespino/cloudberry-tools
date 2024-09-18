import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
from tqdm import tqdm

def process_dly_file(file_path):
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()

        data = []
        for line in lines:
            station_id = line[:11]
            year = int(line[11:15])
            month = int(line[15:17])
            element = line[17:21]
            for day in range(31):
                value = int(line[21+day*8:26+day*8])
                if value != -9999:  # -9999 indicates missing data
                    date = f"{year}-{month:02d}-{day+1:02d}"
                    data.append([station_id, date, element, value])

        df = pd.DataFrame(data, columns=['station_id', 'date', 'element', 'value'])
        output_file = os.path.join(output_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}.csv")
        df.to_csv(output_file, index=False)
        return True
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return False

def main():
    global output_dir
    output_dir = 'processed_ghcn'
    os.makedirs(output_dir, exist_ok=True)

    # Get list of .dly files
    dly_files = [f for f in os.listdir('.') if f.endswith('.dly')]

    # Filter files that don't have a corresponding CSV
    files_to_process = [
        f for f in dly_files
        if not os.path.exists(os.path.join(output_dir, f"{os.path.splitext(f)[0]}.csv"))
    ]

    total_files = len(files_to_process)

    # Determine the number of cores to use (leaving one core free)
    num_cores = max(1, multiprocessing.cpu_count() - 1)

    print(f"Found {len(dly_files)} .dly files.")
    print(f"{total_files} files need processing.")
    print(f"Using {num_cores} cores for processing.")

    try:
        with ProcessPoolExecutor(max_workers=num_cores) as executor:
            futures = [executor.submit(process_dly_file, filename) for filename in files_to_process]

            with tqdm(total=total_files, desc="Processing Progress", unit="file") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    pbar.update(1)

    except KeyboardInterrupt:
        print("\nProcessing interrupted. Progress saved in individual CSV files.")

    finally:
        processed_count = len([f for f in os.listdir(output_dir) if f.endswith('.csv')])
        print(f"\nProcessed data saved to {output_dir}")
        print(f"Total files processed: {processed_count}")
        print(f"Files remaining: {len(dly_files) - processed_count}")

if __name__ == "__main__":
    main()
