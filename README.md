# Chaze Azure function

## Description

This repository holds the implmentation of the azure function, which is responsible for decompressing uploaded data
and creating a new entry in the SQL Database.

Specifically, this function is triggered if a blob is uploaded to a path matching the following pattern: ```compressed/chaze-<num>/<User-ID>/<DD>-<MM>-<YYYY>-<MiMi>-<HH>.txt```.
The container ```compressed``` is the initial hub, where all compressed files are uploaded to. After the processing, a new file is written to the ```trainings``` container.
The blob is then decompressed using the zlib compression standard. See [here](https://tools.ietf.org/html/rfc1950) for the RFC documentation
and [here](http://www.componentace.com/zlib_.NET.htm) for the .NET implementation, which is used in this implementation.
The decompressed file is then processed according to the system used when writing to the flash on the device:
For example, a 2 indicates that a reading of the backside pressure is following. The next 4 bytes hold the sampling time (```unsigned long```)
and the next 4 bytes hold the pressure value (```float```). This is the same for the main pressure, which is indiacted by a ```5```.
The Heart Rate is currently written as an ```uint32_t``` and is indicated by the value 3. Similar to the pressure, the next 4 bytes encode
the sampling time and the next 4 bytes the value (```uint32_t```). The BNO055 is indicated by the value 4. The total number of bytes for the
BNO055 are: 1 byte for the indicator + 4 bytes for time + 7*4 bytes for the data. The 7 data fields are: AccX, AccY, AccZ, QuatW, QuatY, QuatX and QuatZ.
Therefore, the total number is 33.
The values are then extracted and written into a new .txt file named ```<User-ID>/<DD>-<MM>-<YYYY>-<MiMi>-<HH>.txt```. This file is then uploaded
into the ```trainings``` container. After a successful upload, the old blob in the ```compressed``` container is deleted.

## Parameters

- **bool VERBOSE** : If set to true, verbose logging information is provided. If not set, only bare logging is provided, mostly in case of errors.

- **int bufsize** : Can be increased in ```ZOutputStream.cs```. Set to 512 per default. This parameter controls the buffer size used in decompression.

- **int max_to_read** : Currently set to 1024. This is the buffer size used when reading a chunk from the decompressed file. Can be increased for performance, but consumes
more memory.