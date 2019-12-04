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
            using (StreamReader reader = new StreamReader(compressed_file_path))
            {
                string line;
                while (!reader.EndOfStream)
                {
                    line = reader.ReadLine();
                    log.LogInformation($"Line: {line}");
                }
            }
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

        [FunctionName("Process_Data")]
        async public static void Run([BlobTrigger("compressed/{device}-{num}/{name}/{day}-{month}-{year}-{minute}-{hour}.txt", Connection = "AzureWebJobsStorage")]Stream myBlob, string device,
                string num, string name, string day, string month, string year, string minute, string hour, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            log.LogInformation($"Device: {device}-{num} Name: {name}");

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
                    log.LogInformation($"string array is{string.Join(",",byte_values)}");
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

            var file_stream_dec = File.Create(decompressed_file_path);
            file_stream_dec.Close();

            // get duration
            long bytes = myBlob.Length;
            int duration = getDuration(bytes);
            var date = "" + day + "-" + month + "-" + year + "-" + minute + "-" + hour;
            log.LogInformation($"Date: {date}");

            decompress_strings(compressed_file_path, decompressed_file_path, log);
            log.LogInformation($"Decompressed the input string");

            // Need to get user name and fetch or create container for that user
            var user_id = name;
            var container_name = "trainings";
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=trainingsstorage;AccountKey=/Hy9Sk66v2srmQ+Y6u3lZPkrPHSXL0JOOGj48kVmhmPjyBihEbu2G/+zFu7/r7/6E0RVMwLJRm5aGJtl+UEttw==;EndpointSuffix=core.windows.net";
            BlobContainerClient container = new BlobContainerClient(connectionString, container_name);
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

            //! DEBUG, read the file decompressed.dat and print out the chars
            using (FileStream fs = File.OpenRead(decompressed_file_path))
            {
                byte[] b = new byte[1024];
                ASCIIEncoding temp = new ASCIIEncoding();
                while (fs.Read(b,0,b.Length) > 0)
                {
                    log.LogInformation($"{b}");
                    log.LogInformation($"{temp.GetString(b)}");
                }
            }

            if(success)
            {
                // Now get a reference to the blob that we want to create
                var file_name = "" + name + "/" + date + ".txt";
                log.LogInformation($"Uploaded to {file_name}");
                //! Catch blob already exists exception
                BlobClient blob = container.GetBlobClient(file_name);
                // Now, need to upload the decompressed file stream
                await container.UploadBlobAsync(file_name, File.OpenRead(decompressed_file_path));
            }

            //! Please uncomment for deployment.
            // Get the connection string from app settings and use it to create a connection.
            //var str = Environment.GetEnvironmentVariable("sqldb_connection");
            /*var str = "Server=tcp:chazesqlserver.database.windows.net,1433;Initial Catalog=sql-database;Persist Security Info=False;User ID=chaze;Password=Data4Swimmers;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                var text = "INSERT INTO Training (userid, breast, freestyle, butterfly, back, other, distperlen, duration, laps) VALUES ('1', 'false', 'false', 'false', 'false', 'false', '-1', '" + duration + "', '-1')";

                log.LogInformation($"conn opened");

                using (SqlCommand cmd = new SqlCommand(text, conn))
                {
                    log.LogInformation($"inside using");
                    // Execute the command and log the # rows affected.
                    var rows = await cmd.ExecuteNonQueryAsync();
                    log.LogInformation($"{rows} rows were updated");
                }
                log.LogInformation($"end");
            }*/

        }
    }
}
