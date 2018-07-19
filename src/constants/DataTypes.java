package constants;

/**
 * @author RAMA NARAYAN LAKSHMANAN
 *
 */
public class DataTypes {
	public static final int DavisbaseInt = 4;
	public static final int DavisbaseTinyInt = 1;
	public static final int DavisbaseSmallInt = 2;
	public static final int DavisbaseBigInt = 8;
	public static final int DavisbaseReal = 4;
	public static final int DavisbaseDouble = 8;
	public static final int Datetime = 8;
	public static final int Date = 8;
	public static final int Byte1null = 1;
	public static final int Byte2null = 2;
	public static final int Byte4null = 4;
	public static final int Byte8null = 8;

	/**
	 * @param dataType
	 * @return length
	 */
	public static int getDataTypeLength(String dataType) {
		int length = 0;
		switch (dataType.toLowerCase()) {
		case "int":
			length = DavisbaseInt;
			break;
		case "tinyint":
			length = DavisbaseTinyInt;
			break;
		case "smallint":
			length = DavisbaseSmallInt;
			break;
		case "bigint":
			length = DavisbaseBigInt;
			break;
		case "real":
			length = DavisbaseReal;
			break;
		case "double":
			length = DavisbaseDouble;
			break;
		case "datetime":
			length = Datetime;
			break;
		case "date":
			length = Date;
			break;
		case "byte1null":
			length = Byte1null;
			break;
		case "byte2null":
			length = Byte2null;
			break;
		case "byte4null":
			length = Byte4null;
			break;
		case "byte8null":
			length = Byte8null;
			break;

		}
		return length;
	}

	/**
	 * @param dataType
	 * @return null datatype
	 */
	public static String getNullTypeForDataType(String dataType) {
		String nullType = "";
		switch (dataType.toLowerCase()) {
		case "tinyint":
			nullType = "byte1null";
			break;
		case "smallint":
			nullType = "byte2null";
			break;
		case "int":
		case "real":
			nullType = "byte4null";
			break;
		case "bigint":
		case "date":
		case "datetime":
		case "double":
			nullType = "byte8null";
			break;
		default:
			nullType = dataType;
		}
		return nullType;
	}
}
