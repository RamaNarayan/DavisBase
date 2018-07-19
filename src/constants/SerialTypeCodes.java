package constants;

/**
 * @author RAMA NARAYAN LAKSHMANAN
 *
 */
public class SerialTypeCodes {

	public static final int Byte1null = 0;
	public static final int Byte2null = 1;
	public static final int Byte4null = 2;
	public static final int Byte8null = 3;
	public static final int DavisbaseTinyInt = 4;
	public static final int DavisbaseSmallInt = 5;
	public static final int DavisbaseInt = 6;
	public static final int DavisbaseBigInt = 7;
	public static final int DavisbaseReal = 8;
	public static final int DavisbaseDouble = 9;
	public static final int Datetime = 10;
	public static final int Date = 11;
	public static final int Text = 12;

	/**
	 * @param dataType
	 * @return serial code
	 */
	public static int getSerialCodeForDataType(String dataType) {
		int serialCode = 0;
		switch (dataType.toLowerCase()) {
		case "int":
			serialCode = DavisbaseInt;
			break;
		case "tinyint":
			serialCode = DavisbaseTinyInt;
			break;
		case "smallint":
			serialCode = DavisbaseSmallInt;
			break;
		case "bigint":
			serialCode = DavisbaseBigInt;
			break;
		case "real":
			serialCode = DavisbaseReal;
			break;
		case "double":
			serialCode = DavisbaseDouble;
			break;
		case "datetime":
			serialCode = Datetime;
			break;
		case "date":
			serialCode = Date;
			break;
		case "text":
			serialCode = Text;
			break;
		case "byte1null":
			serialCode = Byte1null;
			break;
		case "byte2null":
			serialCode = Byte2null;
			break;
		case "byte4null":
			serialCode = Byte4null;
			break;
		case "byte8null":
			serialCode = Byte8null;
			break;

		}
		return serialCode;
	}

	/**
	 * @param code
	 * @return data type
	 */
	public static String getDataTypeForSerialCode(int code) {
		String dataType = "";
		switch (code) {
		case 0:
			dataType = "Byte1null";
			break;
		case 1:
			dataType = "Byte2null";
			break;
		case 2:
			dataType = "Byte4null";
			break;
		case 3:
			dataType = "Byte8null";
			break;
		case 4:
			dataType = "TinyInt";
			break;
		case 5:
			dataType = "SmallInt";
			break;
		case 6:
			dataType = "Int";
			break;
		case 7:
			dataType = "BigInt";
			break;
		case 8:
			dataType = "Real";
			break;
		case 9:
			dataType = "Double";
			break;
		case 10:
			dataType = "Datetime";
			break;
		case 11:
			dataType = "Date";
			break;
		case 12:
			dataType = "Text";
			break;
		default:
			dataType = "Text";
		}
		return dataType;
	}
}
