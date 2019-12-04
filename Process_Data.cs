using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
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
        
        public static bool VERBOSE = false;
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

        public static void CopyStream(System.IO.Stream input, System.IO.Stream output)
		{
			byte[] buffer = new byte[2000];
			int len;
			while ((len = input.Read(buffer, 0, 2000)) > 0)
			{
				output.Write(buffer, 0, len);
			}
			output.Flush();
		}

        public static void decompress_strings(string compressed_file_path, string decompressed_file_path, ILogger log)
		{
			System.IO.FileStream decompressedFileStream = new System.IO.FileStream(decompressed_file_path, System.IO.FileMode.Create);
            // Create a outZstream object
			zlib.ZOutputStream outZStream = new zlib.ZOutputStream(decompressedFileStream);
			System.IO.FileStream compressedFileStream = new System.IO.FileStream(compressed_file_path, System.IO.FileMode.Open);		
			try
			{
				CopyStream(compressedFileStream, outZStream);
			}
			finally
			{
				outZStream.Close();
			    decompressedFileStream.Close();
				compressedFileStream.Close();
			}
		}

        public enum READ_STATE : byte {PRESSURE=0, BACK_PRESSURE, HEART_RATE, BNO, ERROR};

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
                default:
                    return READ_STATE.ERROR;
            }
        }

        public static int get_left_to_read(READ_STATE curr_state)
        {
            if(curr_state == READ_STATE.BNO)
            {
                return 32;
            } else if(curr_state != READ_STATE.ERROR){
                return 8;
            } else {
                return 8; //TODO: Maybe return 0 so we continue with the next byte to "maybe" recover? Or all hope's lost?
            }
        }

        public static ulong get_time(byte[] time)
        {
            ulong recon = Convert.ToUInt64(16777216*time[0] + 65536*time[1] + 256*time[2] + time[3]);
            return recon;
        }

        public static unsafe float get_float_val(byte[] curr_float_byte)
        {
            //! Check correctness
            long recon = 16777216*curr_float_byte[0] + 65536*curr_float_byte[1] + 256*curr_float_byte[2] + curr_float_byte[3];
            float out_val = *(float *)&recon;
            return out_val;
        }

        public static Int32 get_heart_rate(byte[] curr_byte)
        {
            return 16777216*curr_byte[0] + 65536*curr_byte[1] + 256*curr_byte[2] + curr_byte[3];
        }

        public static void convert_and_write(READ_STATE curr_state, byte[] curr_buf, StreamWriter final_fs, ILogger log)
        {
            if(curr_state == READ_STATE.ERROR){
                if(VERBOSE) log.LogInformation($"Encountered state error");
                return;
            }
            string to_write = "Time: ";
            int len = get_left_to_read(curr_state);
            byte[] time = new byte[4];
            for(int i=0;i<4;i++){
                time[i]=curr_buf[i];
            }
            if(VERBOSE) log.LogInformation($"Time contents are {string.Join(",",time)}");
            ulong time_long = get_time(time);
            to_write += time_long;
            if(VERBOSE) log.LogInformation($"to_write: {to_write}");
            // If BNO, need to fill up more
            if(curr_state == READ_STATE.BNO)
            {
                if(VERBOSE) log.LogInformation($"Have BNO data");
                string[] indicators = new string[] {"AccX: ","AccY: ","AccZ: ","QuatW: ","QuatY: ","QuatX: ","QuatZ: "};
                for(int i=0;i<7;i++)
                {
                    // Starting at i*4 until i*4 + 4
                    byte[] curr_float_byte = new byte[4];
                    for(int j=0;j<4;j++)
                    {
                        curr_float_byte[j] = curr_buf[4+i*4+j]; //Starting at 4 because of the time
                    }
                    if(VERBOSE) log.LogInformation($"{indicators[i]} as byte array is {string.Join(",", curr_float_byte)}");
                    float tmp_val = get_float_val(curr_float_byte);
                    if(VERBOSE) log.LogInformation($"Float is {tmp_val}");
                    to_write += " " + indicators[i] + tmp_val;
                }
            } else {
                string indicator = "";
                if(curr_state == READ_STATE.BACK_PRESSURE)
                    indicator = " Back pressure: ";
                else if(curr_state == READ_STATE.PRESSURE)
                    indicator = " Pressure: ";
                else if(curr_state == READ_STATE.HEART_RATE)
                    indicator = " Heart Rate: ";
                else
                    indicator = " Error: ";

                if(VERBOSE) log.LogInformation($"Not BNO, indicator is {indicator}");

                byte[] tmp_val_array = new byte[4];
                for(int i=0;i<4;i++){
                    tmp_val_array[i]=curr_buf[4+i]; // Plus 4 because of the time
                }
                if(VERBOSE) log.LogInformation($"byte value array of {indicator} is {string.Join(",",tmp_val_array)}");
                if(curr_state == READ_STATE.HEART_RATE)
                {
                    Int32 hr = get_heart_rate(tmp_val_array);
                    if(VERBOSE) log.LogInformation($"Heart rate is {hr}");
                    to_write += indicator + hr;
                } else {
                    float tmp_val = get_float_val(tmp_val_array);
                    if(VERBOSE) log.LogInformation($"Float is {tmp_val}");
                    to_write += indicator + tmp_val;
                }
            }

            // Finally write a new line to the output file
            if(VERBOSE) log.LogInformation($"to_write is {to_write}");
            final_fs.WriteLine(to_write);
        }

        [FunctionName("Process_Data")]
        async public static void Run([BlobTrigger("compressed/{device}-{num}/{name}/{day}-{month}-{year}-{minute}-{hour}.txt", Connection = "AzureWebJobsStorage")]Stream myBlob, string device,
                string num, string name, string day, string month, string year, string minute, string hour, ILogger log)
        {

            if(VERBOSE) log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            if(VERBOSE) log.LogInformation($"Device: {device}-{num} Name: {name}");

            //! This has high memory usage! Use stream in compression.
            string compressed_file_path = "compressed.dat";
            string decompressed_file_path = "decompressed.dat";

            // Write the myBlob stream into memory at location compressed_file_path
            using (var fs = new FileStream(compressed_file_path, FileMode.Create))
            using (var sr = new StreamReader(myBlob))
            {
                /*await sr.CopyToAsync(fileStream);
                fileStream.Close();
                sr.Close();*/
                string line;
                while (!sr.EndOfStream)
                {
                    // Each line contains uint8_t seperated by spaces. Convert them to bytes
                    line = sr.ReadLine();
                    string[] byte_values = line.Split(' ');
                    if(VERBOSE) log.LogInformation($"string array is{string.Join(",",byte_values)}");
                    foreach (var b in byte_values)
                    {
                        byte byteValue;
                        bool successful = Byte.TryParse(b, out byteValue);
                        if(successful)
                        {
                            fs.WriteByte(byteValue);
                        }
                    }
                }
                fs.Seek(0, SeekOrigin.Begin);
            }

            // get duration
            long bytes = myBlob.Length;
            int duration = getDuration(bytes);
            var date = day + "-" + month + "-" + year + "-" + minute + "-" + hour;
            string complete_blob_name = device + "-" + num + "/" + name + "/" + date + ".txt";
            if(VERBOSE) log.LogInformation($"Date: {date}");
            if(VERBOSE) log.LogInformation($"Complete blob name is {complete_blob_name}");

            decompress_strings(compressed_file_path, decompressed_file_path, log);
            if(VERBOSE) log.LogInformation($"Decompressed the input string");

            // Need to get user name and fetch or create container for that user
            var user_id = name;
            var container_name = "trainings";
            var compressed_container_name = "compressed";
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=trainingsstorage;AccountKey=/Hy9Sk66v2srmQ+Y6u3lZPkrPHSXL0JOOGj48kVmhmPjyBihEbu2G/+zFu7/r7/6E0RVMwLJRm5aGJtl+UEttw==;EndpointSuffix=core.windows.net";
            BlobContainerClient container = new BlobContainerClient(connectionString, container_name);
            BlobContainerClient container_compressed = new BlobContainerClient(connectionString, compressed_container_name);
            // Try to creat the container
            bool success = false;
            try {
                await container.CreateAsync();
                success = true;
            }
            catch(RequestFailedException ex)
                when (ex.ErrorCode == BlobErrorCode.ContainerAlreadyExists)
                {
                    // The container was already created
                    success = true;
                }
            catch (RequestFailedException ex)
            {
                Assert.Fail($"Unexpected error: {ex}");
            }
            if(success)
            {
                var old_name = "" + name + "/" + date; // Used for the case where the blob already exists
                var file_name_for_upload = "" + name + "/" + date + ".txt";
                var final_name = "final.txt";
                //! DEBUG, read the file decompressed.dat and print out the bytes
                using (FileStream fs = File.OpenRead(decompressed_file_path))
                using (StreamWriter final_fs = new StreamWriter(final_name))
                {
                    int max_to_read = 1024;
                    byte[] b = new byte[max_to_read];
                    int numBytesToRead = (int)fs.Length;
                    READ_STATE curr_state = READ_STATE.ERROR;
                    int numBytesRead = 0;
                    int left_to_read = 0; // Needs to be used if we collect 1024 values and there is an overlap in the next buffer
                    byte[] curr_buf = new byte[33]; // The maximum number of bytes is 33 from the BNO
                    int curr_buf_offset = 0;
                    while (numBytesToRead > 0)
                    {
                        int n = fs.Read(b, 0, max_to_read);
                        if (n == 0){
                            if(VERBOSE) log.LogInformation($"Read zero bytes. Break");
                            break;
                        }
                        if(VERBOSE) log.LogInformation($"Read {n} bytes.");
                        numBytesRead += n;
                        numBytesToRead -= n;
                        if(VERBOSE) log.LogInformation($"Number of bytes to read is {numBytesToRead}");

                        // We have read n bytes
                        int i = 0;
                        while(i < n)
                        {
                            if(left_to_read == 0) // Update the state
                            {
                                if(VERBOSE) log.LogInformation($"Updating the state");
                                byte first_byte = b[i];
                                if(VERBOSE) log.LogInformation($"First byte is {first_byte}");
                                curr_state = get_state(first_byte);
                                left_to_read = get_left_to_read(curr_state);
                                if(VERBOSE) log.LogInformation($"Left to read: {left_to_read}");
                                curr_buf_offset = 0;
                                i++;
                            } else {
                                if(i + left_to_read < n) // It fits and we can read the next "left_to_read" values
                                {
                                    if(VERBOSE) log.LogInformation($"Can fit everything.");
                                    while(left_to_read > 0)
                                    {
                                        if(VERBOSE) log.LogInformation($"Curr_buf_offset: {curr_buf_offset} i: {i} left_to_read: {left_to_read}");
                                        curr_buf[curr_buf_offset] = b[i];
                                        i++;
                                        curr_buf_offset++;
                                        left_to_read--;
                                    }
                                    //! Need to convert the time and value into long and float(s) and write to file
                                    convert_and_write(curr_state, curr_buf, final_fs,log);
                                } else { // It doesn't fit and we need to wait for the next buffer
                                    while(i < n)
                                    {
                                        curr_buf[curr_buf_offset] = b[i];
                                        i++;
                                        curr_buf_offset++;
                                        left_to_read--;
                                    }
                                }
                            }
                        }
                    }
                }
                // Now get a reference to the blob that we want to create
                if(VERBOSE) log.LogInformation($"Uploaded to {file_name_for_upload}");
                //! Catch blob already exists exception
                BlobClient blob = container.GetBlobClient(file_name_for_upload);
                bool successful_upload = true;
                // Now, need to upload the decompressed file stream
                try {
                    await container.UploadBlobAsync(file_name_for_upload, File.OpenRead(final_name));
                }
                catch(RequestFailedException ex)
                when (ex.ErrorCode == BlobErrorCode.BlobAlreadyExists)
                {
                    // The blob already exists
                    //! Need to upload to the same path, appended with a random number
                    Random rnd = new Random();
                    string new_name = old_name + rnd.Next(1,1000) + ".txt";
                    complete_blob_name = new_name;
                    await container.UploadBlobAsync(new_name, File.OpenRead(final_name));
                }
                catch (RequestFailedException ex)
                {
                    successful_upload = false;
                    log.LogInformation($"Unsuccesful upload of blob {complete_blob_name}");
                }
                if(successful_upload)
                {
                    BlobClient blob_old = container_compressed.GetBlobClient(complete_blob_name);
                    bool res = blob_old.DeleteIfExists();
                    if(!res)
                    {
                        log.LogInformation($"Could not delete blob that was just uplaoded.");
                    } else {
                        if(VERBOSE) log.LogInformation($"Successfully deleted blob just uploaded.");
                    }
                }
            }

            //! Please uncomment for deployment.
            // Get the connection string from app settings and use it to create a connection.
            //var str = Environment.GetEnvironmentVariable("sqldb_connection");
            var str = "Server=tcp:chazesqlserver.database.windows.net,1433;Initial Catalog=sql-database;Persist Security Info=False;User ID=chaze;Password=Data4Swimmers;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                var text = "INSERT INTO Training (userid, breast, freestyle, butterfly, back, other, distperlen, duration, laps) VALUES ('1', 'false', 'false', 'false', 'false', 'false', '-1', '" + duration + "', '-1')";

                if(VERBOSE) log.LogInformation($"conn opened");

                using (SqlCommand cmd = new SqlCommand(text, conn))
                {
                    if(VERBOSE) log.LogInformation($"inside using");
                    // Execute the command and log the # rows affected.
                    var rows = await cmd.ExecuteNonQueryAsync();
                    if(VERBOSE) log.LogInformation($"{rows} rows were updated");
                }
                if(VERBOSE) log.LogInformation($"end");
            }
        }
    }
}
