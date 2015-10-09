package kornell;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;


public class S3EventProcessorUnzip implements RequestHandler<S3Event, String> {

	@Override
	public String handleRequest(S3Event s3Event, Context context) {
		byte[] buffer = new byte[1024];
		try {
			for (S3EventNotificationRecord record: s3Event.getRecords()) {
				String srcBucket = record.getS3().getBucket().getName();

				// Object key may have spaces or unicode non-ASCII characters.
				String srcKey = record.getS3().getObject().getKey()
						.replace('+', ' ');
				srcKey = URLDecoder.decode(srcKey, "UTF-8");

				String dstBucket = srcBucket + "-unzipped";

				// Sanity check: validate that source and destination are different
				// buckets.
				if (srcBucket.equals(dstBucket)) {
					return "";
				}
				
				// Detect file type
	            Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey);
	            if (!matcher.matches()) {
	                System.out.println("Unable to detect file type for key " + srcKey);
	                return "";
	            }
	            String extension = matcher.group(1).toLowerCase();
	            if (!"zip".equals(extension)) {
	                System.out.println("Skipping non-zip file " + srcKey + "with extension " + extension);
	                return "";
	            }
				
				// Download the zip from S3 into a stream
	            AmazonS3 s3Client = new AmazonS3Client();
	            S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
	            ZipInputStream zis = new ZipInputStream(s3Object.getObjectContent());
	            ZipEntry entry = zis.getNextEntry();
	            
	            while(entry != null) {
	            	String fileName = entry.getName();
	            	System.out.println("Extracting " + fileName + ", compressed: " + entry.getCompressedSize() + " bytes, extracted: " + entry.getSize() + " bytes");
	            	ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	            	int len;
	                while ((len = zis.read(buffer)) > 0) {
	                	outputStream.write(buffer, 0, len);
	                }
	                InputStream is = new ByteArrayInputStream(outputStream.toByteArray());
	                ObjectMetadata meta = new ObjectMetadata();
	                meta.setContentLength(outputStream.size());
	                s3Client.putObject(dstBucket, fileName, is, meta);
	                is.close();
	                outputStream.close();
	                entry = zis.getNextEntry();
	            }
	            zis.closeEntry();
	            zis.close();
			}
			return "Ok";
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
