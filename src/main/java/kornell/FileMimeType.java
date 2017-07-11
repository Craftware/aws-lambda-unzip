package kornell;

public enum FileMimeType {
	JPG("jpg", "image/jpeg"),
	JPEG("jepg", "image/jpeg"),
	GIF("gif", "image/gif"),
	PNG("png", "image/png"),
	HTML("html", "text/html"),
	TXT("txt", "text/plain"),
	CSS("css", "text/css"),
	JS("js", "application/javascript"),
	ICO("ico", "image/vnd.microsoft.icon"),
	OTF("otf", "application/font-sfnt"),
	EOT("eot", "application/vnd.ms-fontobject"),
	SVG("svg", "image/svg+xml"),
	TTF("ttf", "application/font-sfnt"),
	WOFF("woff", "application/font-woff"),
	MP4("mp4", "video/mp4"),
	XML("xml", "text/xml"),
	XSD("xsd", "text/xml"),
	
	MISSING("None", "application/octet-stream");
	
	private final String extension;
	private final String mimeType;
	
	FileMimeType(String extension, String contentType) {
		this.extension = extension;
		this.mimeType = contentType;
	}
	
	public String extension() {
		return this.extension;
	}
	
	public String mimeType() {
		return this.mimeType;
	}
	
	public static FileMimeType fromExtension(String extension) {
		for (FileMimeType value: FileMimeType.values()) {
			if (value.extension().equals(extension.toLowerCase())) {
				return value;
			}
		}
		return FileMimeType.MISSING;
	}
}
