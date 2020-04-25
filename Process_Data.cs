using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage;
using NUnit.Framework;
using System.Text;
using System.IO.Compression;


namespace Chaze.Function
{
    public static class Process_Data
    {
        
        public static bool VERBOSE = true;
        public static bool use_TXT = false;
        public static int num_samples_hr_raw = 500;
        public static int CHUNK_SIZE = 8192;
        public static int first_indicator_byte = 24;
        public static int second_indicator_byte = 149;
        public static int getDuration(long bytes) {
            // recording contants
            float RATE_PRESSURE = 30.0f;
            float RATE_IMU = 60.0f;
            float RATE_SPO2 = 0.5f;

            int SIZE_PRESSURE = 8;
            int SIZE_IMU = 32;
            int SIZE_SPO2 = 8;

            float bytespersec = RATE_PRESSURE*SIZE_PRESSURE + RATE_IMU*SIZE_IMU + RATE_SPO2*SIZE_SPO2;

            int duration = (int) (bytes/bytespersec);

            return duration;

        }

        public static void decompress(string compressed_file_path, string decompressed_file_path, ILogger log)
		{
            log.LogInformation($"compressing " + decompressed_file_path);   

			System.IO.FileStream decompressedFileStream = new System.IO.FileStream(decompressed_file_path, System.IO.FileMode.Create);
            // Create a outZstream object
			zlib.ZOutputStream outZStream = new zlib.ZOutputStream(decompressedFileStream);
			System.IO.FileStream compressedFileStream = new System.IO.FileStream(compressed_file_path, System.IO.FileMode.Open);		
			try
			{
				// CopyStream(compressedFileStream, outZStream);
                outZStream.decompressFileStream(compressedFileStream, log, compressed_file_path);
			}
			finally
			{
			    decompressedFileStream.Close();
				compressedFileStream.Close();
			}
		}


        public enum READ_STATE : byte {PRESSURE=0, BACK_PRESSURE, HEART_RATE, BNO, HEART_RATE_RAW, ERROR, EOF=102};

        public static READ_STATE get_state(byte value)
        {
            switch(value){
                case 4:
                    return READ_STATE.BNO;
                case 2:
                    return READ_STATE.BACK_PRESSURE;
                case 5:
                    return READ_STATE.PRESSURE;
                case 3: 
                    return READ_STATE.HEART_RATE;
                case 6:
                    return READ_STATE.HEART_RATE_RAW;
                default:
                    return READ_STATE.ERROR;
            }
        }

        public static int get_state_datasize(READ_STATE state) {
            switch(state){
                case READ_STATE.BNO:
                    return 32;
                case READ_STATE.HEART_RATE_RAW:
                    return 4 + 4*num_samples_hr_raw;
                case READ_STATE.HEART_RATE:
                    return 8;
                case READ_STATE.PRESSURE:
                    return 8;
                case READ_STATE.BACK_PRESSURE:
                    return 8;
                default:
                    return 0; // Return so we look at the next byte. This is for filled up buffers.
            }
        }


        public static ulong get_long(byte[] bytes, int ind, ILogger log) {
            ulong recon = 0;
            try {
                recon = Convert.ToUInt64(16777216*bytes[ind] + 65536*bytes[ind+1] + 256*bytes[ind+2] + bytes[ind+3]);
            } catch(Exception ex) {
                log.LogError($"Couldn't convert to long: {bytes[ind]} {bytes[ind+1]} {bytes[ind+2]} {bytes[ind]+3}. {ex}");
            }
            return recon;
        }

        public static unsafe float get_float(byte[] bytes, int ind)
        {
            //! Check correctness
            long recon = 16777216*bytes[ind] + 65536*bytes[ind+1] + 256*bytes[ind+2] + bytes[ind+3];
            float out_val = *(float *)&recon;
            return out_val;
        }

        public static unsafe Int32 get_int(byte[] bytes, int ind)
        {
            return  16777216*bytes[ind] + 65536*bytes[ind+1] + 256*bytes[ind+2] + bytes[ind+3];
        }

        public static void convert_and_write(READ_STATE curr_state, byte[] data, StreamWriter final_fs, ILogger log)
        {
            if(curr_state == READ_STATE.ERROR){
                log.LogError($"Encountered error state in convert_and_write. Will not write.");
                return;
            }

            // Debug: What data loged on Debug?
            bool log_time       = true;
            bool log_press      = true;
            bool log_back_press = true;
            bool log_bno        = true;
            bool log_heart_raw  = false;
            bool log_heart      = false;


            // get data
            string to_write = "";
            int ind = 0;

            ulong time = get_long(data, ind, log);

            if(log_time) {
                string byte_string = "";
                for(int i = 0; i < 4; i++) {
                    byte_string += data[ind+i] + " ";
                }
                log.LogDebug($"Time: {byte_string} => {time}");
            }

            to_write += "Time: " + time + " ";
            ind += 4;

            // get sensor data
            if(curr_state == READ_STATE.BNO) {
                string[] indicators = new string[] {"AccX: ","AccY: ","AccZ: ","QuatW: ","QuatY: ","QuatX: ","QuatZ: "};
                for(int i = 0; i < 7; i++)
                {
                    float val = get_float(data, ind);

                    if(log_bno) {
                        string byte_string = "";
                        for(int j = 0; j < 4; j++) {
                            byte_string += data[ind+j] + " ";
                        }
                        log.LogDebug($"BNO {indicators[i]}: {byte_string} => {val}");
                    }

                    to_write += indicators[i] + val + " ";
                    ind += 4;
                }
            } else if(curr_state == READ_STATE.HEART_RATE_RAW) {
                to_write += "Heart rate raw: ";

                for(int i = 0; i < num_samples_hr_raw; i++) {
                    Int32 val = get_int(data, ind);

                    if(log_heart_raw) {
                        string byte_string = "";
                        for(int j = 0; j < 4; j++) {
                            byte_string += data[ind+j] + " ";
                        }
                        log.LogDebug($"HR raw sample {i}: {byte_string} => {val}");
                    }

                    to_write += val + " ";
                    ind += 4;
                }
            } else {
                string indicator;
                bool write_log;

                if(curr_state == READ_STATE.BACK_PRESSURE) {
                    indicator = "Back pressure: ";
                    write_log = log_back_press;
                } else if(curr_state == READ_STATE.PRESSURE) {
                    indicator = "Pressure: ";
                    write_log = log_press;
                } else { // curr_state == READ_STATE.HEART_RATE
                    indicator = "Heart Rate: ";
                    write_log = log_heart;
                }

                if(curr_state == READ_STATE.HEART_RATE)
                {
                    Int32 val = get_int(data, ind);

                    if(write_log) {
                        string byte_string = "";
                        for(int j = 0; j < 4; j++) {
                            byte_string += data[ind+j] + " ";
                        }
                        log.LogDebug($"{indicator}{byte_string} => {val}");
                    }

                    to_write += indicator + val + " ";
                    ind += 4;
                } else {
                    float val = get_float(data, ind);

                    if(write_log) {
                        string byte_string = "";
                        for(int j = 0; j < 4; j++) {
                            byte_string += data[ind+j] + " ";
                        }
                        log.LogDebug($"{indicator}{byte_string} => {val}");
                    }

                    to_write += indicator + val + " ";
                    ind += 4;
                }
            }

            // Finally write the line to the output file
            final_fs.WriteLine(to_write);
        }



        [FunctionName("Process_Data")]
        async public static void Run([BlobTrigger("compressed/{device}-{num}/{name}/{day}-{month}-{year}-{minute}-{hour}.txt", Connection = "AzureWebJobsStorage")]Stream myBlob, string device,
                string num, string name, string day, string month, string year, string minute, string hour, ILogger log)
        {

            // get training information
            long bytes = myBlob.Length;
            int duration = getDuration(bytes);
            var date = day + "-" + month + "-" + year + "-" + minute + "-" + hour;
            string complete_blob_name = device + "-" + num + "/" + name + "/" + date + ".txt";

            log.LogInformation($"Triggered by Name:{complete_blob_name} \n Size: {myBlob.Length} Bytes");


            // Write myBlob to file TODO: Direkt myBlob stream
            string compressed_file_path = "compressed.dat";
            string decompressed_file_path = "decompressed.dat";

            if(use_TXT)
            {
                using (var fs = new FileStream(compressed_file_path, FileMode.Create))
                using (var sr = new StreamReader(myBlob))
                {
                    string line;
                    log.LogDebug($"Uploaded (compressed) bytes:");
                    while (!sr.EndOfStream)
                    {
                        // Each line contains uint8_t seperated by spaces. Convert them to bytes
                        line = sr.ReadLine();
                        string[] byte_values = line.Split(' ');
                        log.LogDebug($"{string.Join(",",byte_values)}");
                        foreach (var b in byte_values)
                        {
                            byte byteValue;
                            if(Byte.TryParse(b, out byteValue)) {
                                fs.WriteByte(byteValue);
                            }
                        }
                    }
                    fs.Seek(0, SeekOrigin.Begin);
                }
            } else { // We received a pure byte stream
                using (var fs = new FileStream(compressed_file_path, FileMode.Create))
                {
                    await myBlob.CopyToAsync(fs);
                    fs.Seek(0, SeekOrigin.Begin);
                }
            }


            // decompress
            decompress(compressed_file_path, decompressed_file_path, log);
            log.LogInformation($"Decompressed the input string");


            // Parse bytes of decompressed file into desired format. Write to formated_data
            string formated_data = "formated.txt";

            using (FileStream fs = File.OpenRead(decompressed_file_path))
            using (StreamWriter format_fs = new StreamWriter(formated_data))
            {        
                int to_parse = (int)fs.Length;

                while (to_parse > 0)
                {
                    // get state
                    byte[] state_byte = new byte[1];
                    int n = fs.Read(state_byte, 0, 1); to_parse--;

                    if (n == 0){
                        log.LogCritical($"Read zero bytes. Think this is impossible");
                        break;
                    }

                    log.LogDebug($"State byte: {state_byte[0]}");
                    READ_STATE curr_state = get_state(state_byte[0]);
                    int state_datasize = get_state_datasize(curr_state);
                    log.LogDebug($"State {curr_state}: expecting {state_datasize} bytes");

                    // get data and write to file
                    byte[] data = new byte[state_datasize];
                    fs.Read(data, 0, state_datasize); to_parse -= state_datasize;
                    convert_and_write(curr_state, data, format_fs, log);
                }
            }


            // Get training container
            string connectionString = "DefaultEndpointsProtocol=https;AccountName=trainingsstorage;AccountKey=/Hy9Sk66v2srmQ+Y6u3lZPkrPHSXL0JOOGj48kVmhmPjyBihEbu2G/+zFu7/r7/6E0RVMwLJRm5aGJtl+UEttw==;EndpointSuffix=core.windows.net";
            string container_name = "trainings";
            string compressed_container_name = "compressed";
            BlobContainerClient container = new BlobContainerClient(connectionString, container_name);
            BlobContainerClient container_compressed = new BlobContainerClient(connectionString, compressed_container_name);
            try {       
                await container.CreateIfNotExistsAsync();
            }
            catch (RequestFailedException ex)
            {
                Assert.Fail($"Unexpected error: {ex}");
            }


            // upload formated training to trainings container, user's dir
            string file_name_for_upload = "" + name + "/" + date + ".txt";

            BlobClient blob = container.GetBlobClient(file_name_for_upload);
            bool successful_upload = true;
            try {
                await blob.UploadAsync(File.OpenRead(formated_data));
            }
            catch(RequestFailedException ex)
            when (ex.ErrorCode == BlobErrorCode.BlobAlreadyExists)
            {
                // while there exists a blob with the same name, append '_1'
                bool unique_name = false;
                while(!unique_name) {
                    int str_len = file_name_for_upload.Length;
                    file_name_for_upload = file_name_for_upload.Substring(0, str_len-4) + "_1.txt";
                    blob = container.GetBlobClient(file_name_for_upload);
                    log.LogCritical($"Blob already exists. Trying upload as {file_name_for_upload}");

                    try{
                        await blob.UploadAsync(File.OpenRead(formated_data));
                        unique_name = true;
                    }
                    catch(RequestFailedException ex2)
                    when (ex2.ErrorCode != BlobErrorCode.BlobAlreadyExists) {
                        successful_upload = false;
                        log.LogError($"Cannot upload processed version of {complete_blob_name}: {ex2}");
                    }
                
                }
            }
            catch (RequestFailedException ex)
            {
                successful_upload = false;
                log.LogError($"Cannot upload processed version of {complete_blob_name}: {ex}");
            }


            // Delete compressed blob
            /* if(successful_upload)
            {
                BlobClient blob_old = container_compressed.GetBlobClient(complete_blob_name);
                bool res = blob_old.DeleteIfExists();
                if(!res) {
                    log.LogInformation($"Couldn't delete compressed blob.");
                } else {
                    log.LogInformation($"Deleted compressed blob.");
                }
            } */
        

            // Make DB entry
            // TODO: Get the connection string from app settings and use it to create a connection.
            // var str = Environment.GetEnvironmentVariable("sqldb_connection");
            var str = "Server=tcp:chazesqlserver.database.windows.net,1433;Initial Catalog=sql-database;Persist Security Info=False;User ID=chaze;Password=Data4Swimmers;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            using (SqlConnection conn = new SqlConnection(str))
            {
                bool success_conn = true;
                try {
                    conn.Open();
                } catch (Exception ex)
                {
                    log.LogError($"DB Connection error: {ex}");
                    success_conn = false;
                }
                if(success_conn)
                {
                    log.LogDebug($"DB conn successful");

                    var text = "INSERT INTO Training (userid, breast, freestyle, butterfly, back, other, distperlen, duration, laps) VALUES ('1', 'false', 'false', 'false', 'false', 'false', '-1', '" + duration + "', '-1')";
                    using (SqlCommand cmd = new SqlCommand(text, conn))
                    {
                        // Execute the command and log the # rows affected.
                        var rows = await cmd.ExecuteNonQueryAsync();
                        if(rows != 1) {
                            log.LogError($"{rows} rows were updated in DB, should be 1.");
                        }
                        else {
                            log.LogInformation($"{rows} rows were updated in DB");
                        }
                    }
                }
            }
        }
    }
}