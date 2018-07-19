package utils;

import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import constants.BtreeConstants;
import constants.DataTypes;
import constants.SerialTypeCodes;

/**
 * @author RAMA NARAYAN LAKSHMANAN
 *
 */
public class FileUtils {
	/**
	 * @param payloadLength
	 * @return dataLength - length of the entire record
	 */
	public static int getDataLength(int payloadLength) {
		int dataLength = payloadLength + BtreeConstants.cellHeaderLength;
		return dataLength;
	}

	/**
	 * @param data - list of values
	 * @param dataType - list of corresponding data type
	 * @return payloadLength
	 */
	public static int getPayloadLength(ArrayList<String> data, ArrayList<String> dataType) {
		int lengthOfNoOfColumnsDataTypes = data.size();
		int columnValueSize = 0;
		for (int i = 0; i < dataType.size(); i++) {
			String type = dataType.get(i);
			if (type.equalsIgnoreCase("text")) {
				if (!data.get(i).equalsIgnoreCase("null"))
					columnValueSize = columnValueSize + data.get(i).length() + 1;
			} else {
				columnValueSize = columnValueSize + DataTypes.getDataTypeLength(type);
			}

		}
		int payloadLength = BtreeConstants.lengthOfNoOfColumns + lengthOfNoOfColumnsDataTypes + columnValueSize;

		return payloadLength;
	}

	/**
	 * @param pageBytes
	 * @param dataLength
	 * @param pageSize
	 * @return whether the page is full
	 */
	public static boolean isPageFull(byte[] pageBytes, int dataLength, long pageSize) {
		short startOfContentbufferValue = BufferManagement.readShort(pageBytes, BtreeConstants.startOfContentOffset);
		long startOfContent = startOfContentbufferValue == 0 ? pageSize : startOfContentbufferValue;

		if (startOfContent > 0) {
			startOfContent = (int) pageSize - startOfContent;
		}
		int noOfCells = pageBytes[BtreeConstants.noOfCellsOffset];

		long recordPointerLoc = BtreeConstants.pointerOffset + 2 * noOfCells + 2;

		int availableSpace = (int) (pageSize - ((int) recordPointerLoc + (int) startOfContent + dataLength));

		boolean returnValue = true;
		if (availableSpace >= 0) {
			returnValue = false;
		}
		return returnValue;
	}

	/**
	 * @param node
	 * @param rowId
	 * @param data
	 * @param dataType
	 * @param pageSize
	 * @return page with inserted data
	 */
	public static byte[] insertDataInDataPage(byte[] node, int rowId, ArrayList<String> data,
			ArrayList<String> dataType, long pageSize) {
		int payloadLength = FileUtils.getPayloadLength(data, dataType);
		int dataLength = FileUtils.getDataLength(payloadLength);
		int noOfCells = node[BtreeConstants.noOfCellsOffset];
		int recordPointerLoc = BtreeConstants.pointerOffset + 2 * noOfCells;
		// write no of cells
		noOfCells = noOfCells + 1;
		node[BtreeConstants.noOfCellsOffset] = (byte) noOfCells;
		// write start of content
		short startOfContentbufferValue = BufferManagement.readShort(node, BtreeConstants.startOfContentOffset);
		int currentStartOfContent = startOfContentbufferValue;
		long currentStartOfContentLoc = currentStartOfContent == 0 ? pageSize : currentStartOfContent;
		long seekPos = currentStartOfContentLoc - dataLength;

		// write start of content
		node = BufferManagement.writeShort(node, BtreeConstants.startOfContentOffset, (short) seekPos);
		// write record pointer
		node = BufferManagement.writeShort(node, recordPointerLoc, (short) seekPos);

		// write cell data
		// write cell header
		int seekPosIndex = (int) seekPos;
		node = BufferManagement.writeShort(node, seekPosIndex, (short) payloadLength);
		node = BufferManagement.writeInt(node, seekPosIndex + 2, rowId);
		// write data
		node[seekPosIndex + 6] = (byte) data.size();
		int nextIndex = seekPosIndex + 7;
		for (int i = 0; i < data.size(); i++) {
			String type = dataType.get(i);
			if (!type.equalsIgnoreCase("text")) {
				node[nextIndex] = (byte) SerialTypeCodes.getSerialCodeForDataType(type);
			} else {
				if (!data.get(i).equalsIgnoreCase("null"))
					node[nextIndex] = (byte) (SerialTypeCodes.getSerialCodeForDataType(type) + data.get(i).length()
							+ 1);
				else {
					node[nextIndex] = (byte) (SerialTypeCodes.getSerialCodeForDataType(type));
				}
			}
			nextIndex = nextIndex + 1;
		}
		for (int i = 0; i < data.size(); i++) {
			switch (dataType.get(i).toLowerCase()) {
			case "byte1null":
				node = BufferManagement.write(node, nextIndex, (byte) 0);
				nextIndex = nextIndex + 1;
				break;
			case "byte2null":
				node = BufferManagement.writeShort(node, nextIndex, (short) 0);
				nextIndex = nextIndex + 2;
				break;
			case "byte4null":
				node = BufferManagement.writeInt(node, nextIndex, (int) 0);
				nextIndex = nextIndex + 4;
				break;
			case "byte8null":
				node = BufferManagement.writeLong(node, nextIndex, (long) 0);
				nextIndex = nextIndex + 8;
				break;
			case "tinyint":
				node = BufferManagement.write(node, nextIndex, Byte.parseByte(data.get(i)));
				nextIndex = nextIndex + 1;
				break;
			case "smallint":
				node = BufferManagement.writeShort(node, nextIndex, Short.parseShort(data.get(i)));
				nextIndex = nextIndex + 2;
				break;
			case "int":
				node = BufferManagement.writeInt(node, nextIndex, Integer.parseInt(data.get(i)));
				nextIndex = nextIndex + 4;
				break;
			case "bigint":
				node = BufferManagement.writeLong(node, nextIndex, (long) Long.parseLong(data.get(i)));
				nextIndex = nextIndex + 8;
				break;
			case "real":
				node = BufferManagement.writeFloat(node, nextIndex, (float) Float.parseFloat(data.get(i)));
				nextIndex = nextIndex + 4;
				break;
			case "double":
				node = BufferManagement.writeDouble(node, nextIndex, (double) Double.parseDouble(data.get(i)));
				nextIndex = nextIndex + 8;
				break;
			case "datetime":
				node = BufferManagement.writeLong(node, nextIndex, (long) Long.parseLong(data.get(i)));
				nextIndex = nextIndex + 8;
				break;
			case "date":
				node = BufferManagement.writeLong(node, nextIndex, (long) Long.parseLong(data.get(i)));
				nextIndex = nextIndex + 8;
				break;
			case "text":
				if (!data.get(i).equalsIgnoreCase("null")) {
					node = BufferManagement.writeString(node, nextIndex, data.get(i));
					nextIndex = nextIndex + data.get(i).length() + 1;
				}
				break;

			}
		}

		return node;
	}

	/**
	 * @param pageBytes
	 * utility function to print page
	 */
	public static void printPage(byte[] pageBytes) {
		for (int i = 0; i < pageBytes.length; i++) {
			System.out.print((int) pageBytes[i]);
			System.out.print("\t");
			if ((i + 1) % 16 == 0)
				System.out.println();
		}
	}

	/**
	 * @param rowId
	 * @param data
	 * @param dataType
	 * @param pageSize
	 * @return new leaf page
	 */
	public static byte[] createNewLeafPage(int rowId, ArrayList<String> data, ArrayList<String> dataType,
			long pageSize) {
		byte[] newLeafNode = new byte[(int) pageSize];
		newLeafNode = FileUtils.insertDataInDataPage(newLeafNode, rowId, data, dataType, pageSize);
		newLeafNode[0] = (byte) 0x0D;
		newLeafNode[BtreeConstants.rightOffset] = (byte) 0xFF;
		newLeafNode[BtreeConstants.rightOffset + 1] = (byte) 0xFF;
		newLeafNode[BtreeConstants.rightOffset + 2] = (byte) 0xFF;
		newLeafNode[BtreeConstants.rightOffset + 3] = (byte) 0xFF;
		return newLeafNode;
	}

	/**
	 * @param node
	 * @param rightSiblingPageNumber
	 * @return page with right sibling written
	 */
	public static byte[] writeRightSibling(byte[] node, long rightSiblingPageNumber) {

		node = BufferManagement.writeInt(node, BtreeConstants.rightOffset, (int) rightSiblingPageNumber);
		return node;
	}

	/**
	 * @param leftPageNumber
	 * @param key
	 * @param rightPageNumber
	 * @param pageSize
	 * @return new internal node page
	 */
	public static byte[] createNewInternalNode(int leftPageNumber, int key, long rightPageNumber, long pageSize) {

		byte[] newInternalNode = new byte[(int) pageSize];
		newInternalNode = FileUtils.insertDataInInternalPage(newInternalNode, key, pageSize, rightPageNumber,
				leftPageNumber);
		newInternalNode[0] = (byte) 0x05;
		return newInternalNode;
	}

	/**
	 * @param node
	 * @param key
	 * @param pageSize
	 * @param rightPageNumber
	 * @param leftPageNumber
	 * @return page with data inserted
	 */
	public static byte[] insertDataInInternalPage(byte[] node, int key, long pageSize, long rightPageNumber,
			long leftPageNumber) {

		int dataLength = BtreeConstants.interiorCellLength;
		int noOfCells = node[BtreeConstants.noOfCellsOffset];
		int recordPointerLoc = BtreeConstants.pointerOffset + 2 * noOfCells;
		// write no of cells
		noOfCells = noOfCells + 1;
		node[BtreeConstants.noOfCellsOffset] = (byte) noOfCells;
		// write start of content
		short startOfContentbufferValue = BufferManagement.readShort(node, BtreeConstants.startOfContentOffset);

		int currentStartOfContent = startOfContentbufferValue;
		long currentStartOfContentLoc = currentStartOfContent == 0 ? pageSize : currentStartOfContent;
		long seekPos = currentStartOfContentLoc - dataLength;

		node = BufferManagement.writeShort(node, BtreeConstants.startOfContentOffset, (short) seekPos);
		// write record pointer
		node = BufferManagement.writeShort(node, recordPointerLoc, (short) seekPos);

		int seekPosIndex = (int) seekPos;
		if (leftPageNumber != 0) {
			node = BufferManagement.writeInt(node, seekPosIndex, (int) leftPageNumber);

		} else {
			int newLeft = BufferManagement.readInt(node, BtreeConstants.rightOffset);

			node = BufferManagement.writeInt(node, seekPosIndex, newLeft);
			node = BufferManagement.writeInt(node, BtreeConstants.rightOffset, (int) leftPageNumber);

		}

		node = BufferManagement.writeInt(node, seekPosIndex + 4, key - 1);
		// write right child pointer
		node = writeRightSibling(node, rightPageNumber);
		return node;
	}

	/**
	 * @param file
	 * @param pageSize
	 * @return first data page address
	 */
	public static int getFirstLeafPageAddress(RandomAccessFile file, long pageSize) {
		int pageNumber = 0;
		try {
			file.seek(0);
			int nodePage = file.readInt();

			byte[] node = new byte[(int) (pageSize)];
			file.seek(nodePage);
			file.read(node, 0, (int) pageSize);
			int noOfCells = node[BtreeConstants.noOfCellsOffset];
			while ((int) node[0] != 13) {
				noOfCells = node[BtreeConstants.noOfCellsOffset];
				int firstPointer = (int) BufferManagement.readShort(node, BtreeConstants.pointerOffset);
				nodePage = BufferManagement.readInt(node, firstPointer);
				file.seek(nodePage);
				file.read(node, 0, (int) pageSize);
			}
			pageNumber = nodePage;
		} catch (Exception e) {
			System.out.println(e);
		}

		return pageNumber;
	}

	/**
	 * @param node
	 * @param dataType
	 * @param length
	 * @param offset
	 * @return a column value
	 */
	public static String readColumn(byte[] node, String dataType, int length, int offset) {

		String column;
		switch (dataType.toLowerCase()) {
		case "int":
			Integer columnInt = BufferManagement.readInt(node, offset);
			column = columnInt.toString();
			break;
		case "tinyint":
			Byte columnByte = BufferManagement.read(node, offset);
			column = columnByte.toString();
			break;
		case "smallint":
			Short columnShort = BufferManagement.readShort(node, offset);
			column = columnShort.toString();
			break;
		case "bigint":
			Long columnLong = BufferManagement.readLong(node, offset);
			column = columnLong.toString();
			break;
		case "real":
			Float columnFloat = BufferManagement.readFloat(node, offset);
			column = columnFloat.toString();
			break;
		case "double":
			Double columnDouble = BufferManagement.readDouble(node, offset);
			column = columnDouble.toString();
			break;
		case "datetime":
			Long columnDateTime = BufferManagement.readLong(node, offset);
			column = columnDateTime.toString();
			break;
		case "date":
			Long columnDate = BufferManagement.readLong(node, offset);
			column = columnDate.toString();
			break;
		case "byte1null":
			column = null;
			break;
		case "byte2null":
			column = null;
			break;
		case "byte4null":
			column = null;
			break;
		case "byte8null":
			column = null;
			break;
		case "text":
			if (length != 0)
				column = BufferManagement.readString(node, offset);
			else
				column = "null";
			break;
		default:
			column = BufferManagement.readString(node, offset);
		}
		return column;
	}

	/**
	 * @param filename
	 * @param searchQuery
	 * @param noOfTableColumns
	 * @param pageSize
	 * @return data from columns table
	 */
	public static ArrayList<ArrayList<String>> getRecordsFromColumnsTable(String filename, String searchQuery,
			int noOfTableColumns, long pageSize) {

		ArrayList<ArrayList<String>> records = new ArrayList<ArrayList<String>>();
		RandomAccessFile columnsFile;

		try {
			columnsFile = new RandomAccessFile(filename, "rw");
			int pageNumber = FileUtils.getFirstLeafPageAddress(columnsFile, pageSize);
			int availableColumns = noOfTableColumns;
			int loc = 0;
			while (pageNumber != -1) {
				byte[] node = new byte[(int) pageSize];
				columnsFile.seek(pageNumber);
				columnsFile.read(node, 0, (int) pageSize);
				int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
				int pointerArray[] = new int[noOfCells];
				int pointerLoc = BtreeConstants.pointerOffset;
				for (int i = 0; i < noOfCells; i++) {
					pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
					pointerLoc = pointerLoc + 2;
				}
				for (int pointer : pointerArray) {

					int noOfColumns = BufferManagement.read(node, pointer + BtreeConstants.cellHeaderLength);
					loc = pointer + BtreeConstants.cellHeaderLength + noOfColumns + BtreeConstants.lengthOfNoOfColumns;
					int off = pointer + BtreeConstants.cellHeaderLength + BtreeConstants.lengthOfNoOfColumns;
					int length = BufferManagement.read(node, off);

					String columnString = readColumn(node, "text", length, loc);
					int isActivePosition = 6;
					for (int i = 1; i < isActivePosition; i++) {
						int serialCode = BufferManagement.read(node, off);
						length = getLengthOfSerialCode(serialCode);
						loc = loc + length;
						off = off + 1;
					}
					String isActiveString = readColumn(node, "tinyint", length, loc);
					if (searchQuery.equalsIgnoreCase(columnString) && isActiveString.equals("1")) {
						records.add(readRecordFromNode(node, pointer));
						availableColumns = availableColumns - 1;
					}
					if (availableColumns == 0) {
						break;
					}

				}
				if (availableColumns == 0) {
					break;
				}
				pageNumber = BufferManagement.readInt(node, BtreeConstants.rightOffset);
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return records;
	}

	/**
	 * @param node
	 * @param pointer
	 * @return reads a record from data page
	 */
	public static ArrayList<String> readRecordFromNode(byte[] node, int pointer) {

		int noOfColumns = BufferManagement.read(node, pointer + BtreeConstants.cellHeaderLength);
		ArrayList<String> record = new ArrayList<String>();
		Integer rowId = BufferManagement.readInt(node, pointer + 2);
		record.add(rowId.toString());
		int colIndex = pointer + BtreeConstants.cellHeaderLength + BtreeConstants.lengthOfNoOfColumns;
		int offset = colIndex + noOfColumns;
		for (int i = 0; i < noOfColumns; i++) {
			int serialCode = BufferManagement.read(node, colIndex + i);
			String dataType = SerialTypeCodes.getDataTypeForSerialCode(serialCode);
			int length = getLengthOfSerialCode(serialCode);
			String column = FileUtils.readColumn(node, dataType, length, offset);
			if (dataType.equalsIgnoreCase("date") || dataType.equalsIgnoreCase("datetime")) {
				String datetime = getDateTimeFromEpoch(Long.parseLong(column));
				if (dataType.equalsIgnoreCase("date")) {
					String[] temp = datetime.split("_");
					record.add(temp[0]);
				} else {
					record.add(datetime);
				}
			} else {
				record.add(column);
			}
			offset = offset + length;

		}
		return record;
	}

	/**
	 * @param serialCode
	 * @return serialCodeLength
	 */
	public static int getLengthOfSerialCode(int serialCode) {
		String dataType = SerialTypeCodes.getDataTypeForSerialCode(serialCode);
		int length;
		if (dataType.equalsIgnoreCase("text")) {
			length = serialCode - SerialTypeCodes.Text;
		} else {
			length = DataTypes.getDataTypeLength(dataType);
		}

		return length;
	}

	/**
	 * @param filename
	 * @param searchQuery
	 * @param searchDataType
	 * @param ordinalPosition
	 * @param operator
	 * @param pageSize
	 * @return records for user table
	 */
	public static ArrayList<ArrayList<String>> getRecordsFromUserTable(String filename, String searchQuery,
			String searchDataType, int ordinalPosition, String operator, long pageSize) {

		ArrayList<ArrayList<String>> records = new ArrayList<ArrayList<String>>();
		RandomAccessFile tableFile;

		try {
			tableFile = new RandomAccessFile(filename, "rw");
			int pageNumber = FileUtils.getFirstLeafPageAddress(tableFile, pageSize);
			int loc = 0;
			while (pageNumber != -1) {
				byte[] node = new byte[(int) pageSize];
				tableFile.seek(pageNumber);
				tableFile.read(node, 0, (int) pageSize);
				int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
				int pointerArray[] = new int[noOfCells];
				int pointerLoc = BtreeConstants.pointerOffset;
				for (int i = 0; i < noOfCells; i++) {
					pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
					pointerLoc = pointerLoc + 2;
				}
				for (int pointer : pointerArray) {

					int noOfColumns = BufferManagement.read(node, pointer + BtreeConstants.cellHeaderLength);
					loc = pointer + BtreeConstants.cellHeaderLength + noOfColumns + BtreeConstants.lengthOfNoOfColumns;
					int off = pointer + BtreeConstants.cellHeaderLength + BtreeConstants.lengthOfNoOfColumns;
					int length = BufferManagement.read(node, off);
					int ordOff = off + ordinalPosition - 1;
					int sCode = BufferManagement.read(node, ordOff);
					String columnString = "";

					if (ordinalPosition > 1) {
						for (int i = 1; i < ordinalPosition; i++) {
							int serialCode = BufferManagement.read(node, off);
							length = getLengthOfSerialCode(serialCode);
							loc = loc + length;
							off = off + 1;
						}
					}

					if (!(searchDataType.equalsIgnoreCase("text") && sCode == 12)) {
						columnString = readColumn(node, searchDataType, length, loc);
					} else {
						columnString = null;
					}
					
					String columnDataType = SerialTypeCodes.getDataTypeForSerialCode(sCode);
					boolean isSearchSuccess = search(searchQuery, searchDataType, columnString, columnDataType,operator);
					if (isSearchSuccess) {
						records.add(readRecordFromNode(node, pointer));
					}
				}
				pageNumber = BufferManagement.readInt(node, BtreeConstants.rightOffset);
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return records;
	}

	/**
	 * @param searchQuery
	 * @param searchDataType
	 * @param columnString
	 * @param operator
	 * @return if search result is success
	 */
	private static boolean search(String searchQuery, String searchDataType, String columnString, String columnDataType,String operator) {

		boolean isSearchSuccess = false;
		switch (searchDataType) {
		case "int":
			int searchValue = Integer.parseInt(searchQuery);
			int columnValue = Integer.parseInt(columnString);
			if(searchDataType.equalsIgnoreCase(columnDataType)) {
				switch (operator) {
				case "=":
					if (columnValue == searchValue) {
						isSearchSuccess = true;
					}
					break;
				case "<":
					if (columnValue < searchValue) {
						isSearchSuccess = true;
					}
					break;
				case "<=":
					if (columnValue <= searchValue) {
						isSearchSuccess = true;
					}
					break;
				case ">":
					if (columnValue > searchValue) {
						isSearchSuccess = true;
					}
					break;
				case ">=":
					if (columnValue >= searchValue) {
						isSearchSuccess = true;
					}
					break;
				case "!=":
				case "<>":
					if (columnValue != searchValue) {
						isSearchSuccess = true;
					}
					break;
				}
			}
			break;
		case "tinyint":
			byte searchByteValue = Byte.parseByte(searchQuery);
			byte columnByteValue = Byte.parseByte(columnString);
			if(searchDataType.equalsIgnoreCase(columnDataType)) {
				switch (operator) {
				case "=":
					if (columnByteValue == searchByteValue) {
						isSearchSuccess = true;
					}
					break;
				case "<":
					if (columnByteValue < searchByteValue) {
						isSearchSuccess = true;
					}
					break;
				case "<=":
					if (columnByteValue <= searchByteValue) {
						isSearchSuccess = true;
					}
					break;
				case ">":
					if (columnByteValue > searchByteValue) {
						isSearchSuccess = true;
					}
					break;
				case ">=":
					if (columnByteValue >= searchByteValue) {
						isSearchSuccess = true;
					}
					break;
				case "!=":
				case "<>":
					if (columnByteValue != searchByteValue) {
						isSearchSuccess = true;
					}
					break;
				}
			}
			break;
		case "smallint":
			short searchShortValue = Short.parseShort(searchQuery);
			short columnShortValue = Short.parseShort(columnString);
			if(searchDataType.equalsIgnoreCase(columnDataType)) {
				switch (operator) {
				case "=":
					if (columnShortValue == searchShortValue) {
						isSearchSuccess = true;
					}
					break;
				case "<":
					if (columnShortValue < searchShortValue) {
						isSearchSuccess = true;
					}
					break;
				case "<=":
					if (columnShortValue <= searchShortValue) {
						isSearchSuccess = true;
					}
					break;
				case ">":
					if (columnShortValue > searchShortValue) {
						isSearchSuccess = true;
					}
					break;
				case ">=":
					if (columnShortValue >= searchShortValue) {
						isSearchSuccess = true;
					}
					break;
				case "!=":
				case "<>":
					if (columnShortValue != searchShortValue) {
						isSearchSuccess = true;
					}
					break;
				}
			}			
			break;
		case "bigint":
			long searchLongValue = Long.parseLong(searchQuery);
			long columnLongValue = Long.parseLong(columnString);
			if(searchDataType.equalsIgnoreCase(columnDataType)) {
				switch (operator) {
				case "=":
					if (columnLongValue == searchLongValue) {
						isSearchSuccess = true;
					}
					break;
				case "<":
					if (columnLongValue < searchLongValue) {
						isSearchSuccess = true;
					}
					break;
				case "<=":
					if (columnLongValue <= searchLongValue) {
						isSearchSuccess = true;
					}
					break;
				case ">":
					if (columnLongValue > searchLongValue) {
						isSearchSuccess = true;
					}
					break;
				case ">=":
					if (columnLongValue >= searchLongValue) {
						isSearchSuccess = true;
					}
					break;
				case "<>":
				case "!=":
					if (columnLongValue != searchLongValue) {
						isSearchSuccess = true;
					}
					break;
				}
			}
			
			break;
		case "real":
			float searchFloatValue = Float.parseFloat(searchQuery);
			float columnFloatValue = Float.parseFloat(columnString);
			if(searchDataType.equalsIgnoreCase(columnDataType)) {
				switch (operator) {
				case "=":
					if (columnFloatValue == searchFloatValue) {
						isSearchSuccess = true;
					}
					break;
				case "<":
					if (columnFloatValue < searchFloatValue) {
						isSearchSuccess = true;
					}
					break;
				case "<=":
					if (columnFloatValue <= searchFloatValue) {
						isSearchSuccess = true;
					}
					break;
				case ">":
					if (columnFloatValue > searchFloatValue) {
						isSearchSuccess = true;
					}
					break;
				case ">=":
					if (columnFloatValue >= searchFloatValue) {
						isSearchSuccess = true;
					}
					break;
				case "<>":
				case "!=":
					if (columnFloatValue != searchFloatValue) {
						isSearchSuccess = true;
					}
					break;
				}
			}
			
			break;
		case "double":
			double searchDoubleValue = Double.parseDouble(searchQuery);
			double columnDoubleValue = Double.parseDouble(columnString);
			if(searchDataType.equalsIgnoreCase(columnDataType)) {
				switch (operator) {
				case "=":
					if (columnDoubleValue == searchDoubleValue) {
						isSearchSuccess = true;
					}
					break;
				case "<":
					if (columnDoubleValue < searchDoubleValue) {
						isSearchSuccess = true;
					}
					break;
				case "<=":
					if (columnDoubleValue <= searchDoubleValue) {
						isSearchSuccess = true;
					}
					break;
				case ">":
					if (columnDoubleValue > searchDoubleValue) {
						isSearchSuccess = true;
					}
					break;
				case ">=":
					if (columnDoubleValue >= searchDoubleValue) {
						isSearchSuccess = true;
					}
					break;
				case "<>":
				case "!=":
					if (columnDoubleValue != searchDoubleValue) {
						isSearchSuccess = true;
					}
					break;
				}
			}
			
			break;
		case "datetime":
			long searchDatetimeValue = Long.parseLong(searchQuery);
			long columnDatetimeValue = Long.parseLong(columnString);
			if(searchDataType.equalsIgnoreCase(columnDataType)) {
				switch (operator) {
				case "=":
					if (columnDatetimeValue == searchDatetimeValue) {
						isSearchSuccess = true;
					}
					break;
				case "<":
					if (columnDatetimeValue < searchDatetimeValue) {
						isSearchSuccess = true;
					}
					break;
				case "<=":
					if (columnDatetimeValue <= searchDatetimeValue) {
						isSearchSuccess = true;
					}
					break;
				case ">":
					if (columnDatetimeValue > searchDatetimeValue) {
						isSearchSuccess = true;
					}
					break;
				case ">=":
					if (columnDatetimeValue >= searchDatetimeValue) {
						isSearchSuccess = true;
					}
					break;
				case "<>":
				case "!=":
					if (columnDatetimeValue != searchDatetimeValue) {
						isSearchSuccess = true;
					}
					break;
				}

			}
			
			break;
		case "date":
			long searchDateValue = Long.parseLong(searchQuery);
			long columnDateValue = Long.parseLong(columnString);
			if(searchDataType.equalsIgnoreCase(columnDataType)) {
				switch (operator) {
				case "=":
					if (columnDateValue == searchDateValue) {
						isSearchSuccess = true;
					}
					break;
				case "<":
					if (columnDateValue < searchDateValue) {
						isSearchSuccess = true;
					}
					break;
				case "<=":
					if (columnDateValue <= searchDateValue) {
						isSearchSuccess = true;
					}
					break;
				case ">":
					if (columnDateValue > searchDateValue) {
						isSearchSuccess = true;
					}
					break;
				case ">=":
					if (columnDateValue >= searchDateValue) {
						isSearchSuccess = true;
					}
					break;
				case "<>":
				case "!=":
					if (columnDateValue != searchDateValue) {
						isSearchSuccess = true;
					}
					break;
				}
			}
			
			break;
		case "text":
			switch (operator) {
			case "=":
				if (columnString!=null&&searchQuery.equalsIgnoreCase(columnString)) {
					isSearchSuccess = true;
				}
				break;
			case "<":
				if (columnString!=null&&columnString.compareTo(searchQuery)<0) {
					isSearchSuccess = true;
				}
				break;
			case "<=":
				if (columnString!=null&&columnString.compareTo(searchQuery)<=0) {
					isSearchSuccess = true;
				}
				break;
			case ">":
				if (columnString!=null&&columnString.compareTo(searchQuery)>0) {
					isSearchSuccess = true;
				}
				break;
			case ">=":
				if (columnString!=null&&columnString.compareTo(searchQuery)>=0) {
					isSearchSuccess = true;
				}
				break;
			case "like":
				String searchValueString = searchQuery;
				if(columnString!=null) {
					if (searchValueString.contains("%") || searchValueString.contains("_")) {
						searchQuery = searchValueString.replaceAll("%", ".*").replaceAll("_", ".");
						if (columnString.matches(searchQuery)) {
							isSearchSuccess = true;
						}
					} else {
						if (searchQuery.equalsIgnoreCase(columnString)) {
							isSearchSuccess = true;
						}
					}
				}
				break;
				
			case "<>":
			case "!=":
				if (columnString!=null&&!searchQuery.equalsIgnoreCase(columnString)) {
					isSearchSuccess = true;
				}
				break;
			case "is": if(searchQuery.equalsIgnoreCase("null")) {
				if(columnString==null) {
					isSearchSuccess = true;
				}
			}
			else {
				if(columnString!=null) {
					isSearchSuccess = true;
				}
			}
				break;
			}
			
			break;
		case "byte1null":
			if(operator.equalsIgnoreCase("is")) {
				if(searchQuery.equalsIgnoreCase("null")) {
					if(searchDataType.equalsIgnoreCase(columnDataType)) {
						isSearchSuccess = true;
					}
				}
				else {
					if(!searchDataType.equalsIgnoreCase(columnDataType)) {
						isSearchSuccess = true;
					}
				}
			}
			break;
		case "byte2null":
			if(operator.equalsIgnoreCase("is")) {
				if(searchQuery.equalsIgnoreCase("null")) {
					if(searchDataType.equalsIgnoreCase(columnDataType)) {
						isSearchSuccess = true;
					}
				}
				else {
					if(!searchDataType.equalsIgnoreCase(columnDataType)) {
						isSearchSuccess = true;
					}
				}
			}
			break;
		case "byte4null":
			if(operator.equalsIgnoreCase("is")) {
				if(searchQuery.equalsIgnoreCase("null")) {
					if(searchDataType.equalsIgnoreCase(columnDataType)) {
						isSearchSuccess = true;
					}
				}
				else {
					if(!searchDataType.equalsIgnoreCase(columnDataType)) {
						isSearchSuccess = true;
					}
				}
			}
			break;
		case "byte8null":
			if(operator.equalsIgnoreCase("is")) {
				if(searchQuery.equalsIgnoreCase("null")) {
					if(searchDataType.equalsIgnoreCase(columnDataType)) {
						isSearchSuccess = true;
					}
				}
				else {
					if(!searchDataType.equalsIgnoreCase(columnDataType)) {
						isSearchSuccess = true;
					}
				}
			}
			break;

		}		

		return isSearchSuccess;
	}

	/**
	 * @param filename
	 * @param pageSize
	 * @return all records for a user table
	 */
	public static ArrayList<ArrayList<String>> getAllRecordsFromUserTable(String filename, long pageSize) {
		ArrayList<ArrayList<String>> records = new ArrayList<ArrayList<String>>();
		RandomAccessFile tableFile;
		try {
			tableFile = new RandomAccessFile(filename, "rw");
			int pageNumber = FileUtils.getFirstLeafPageAddress(tableFile, pageSize);
			while (pageNumber != -1) {
				byte[] node = new byte[(int) pageSize];
				tableFile.seek(pageNumber);
				tableFile.read(node, 0, (int) pageSize);
				int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
				int pointerArray[] = new int[noOfCells];
				int pointerLoc = BtreeConstants.pointerOffset;
				for (int i = 0; i < noOfCells; i++) {
					pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
					pointerLoc = pointerLoc + 2;
				}
				for (int pointer : pointerArray) {
					records.add(readRecordFromNode(node, pointer));
				}
				pageNumber = BufferManagement.readInt(node, BtreeConstants.rightOffset);
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return records;
	}

	/**
	 * @param filename
	 * @param searchQuery
	 * @param searchDataType
	 * @param searchOrdinalPosition
	 * @param updateValue
	 * @param updateDataType
	 * @param updateOrdinalPosition
	 * @param operator
	 * @param pageSize
	 * @return count of records updated
	 */
	public static int updateRecordsInUserTable(String filename, String searchQuery, String searchDataType,
			int searchOrdinalPosition, String updateValue, String updateDataType, int updateOrdinalPosition,
			String operator, long pageSize) {

		RandomAccessFile tableFile;
		int count = 0;
		byte[] node = new byte[(int) pageSize];
		try {
			tableFile = new RandomAccessFile(filename, "rw");
			int pageNumber = FileUtils.getFirstLeafPageAddress(tableFile, pageSize);
			int loc = 0;
			while (pageNumber != -1) {
				tableFile.seek(pageNumber);
				tableFile.read(node, 0, (int) pageSize);
				int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
				int pointerArray[] = new int[noOfCells];
				int pointerLoc = BtreeConstants.pointerOffset;
				for (int i = 0; i < noOfCells; i++) {
					pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
					pointerLoc = pointerLoc + 2;
				}
				for (int pointer : pointerArray) {
					int noOfColumns = BufferManagement.read(node, pointer + BtreeConstants.cellHeaderLength);
					loc = pointer + BtreeConstants.cellHeaderLength + noOfColumns + BtreeConstants.lengthOfNoOfColumns;
					int off = pointer + BtreeConstants.cellHeaderLength + BtreeConstants.lengthOfNoOfColumns;
					int length = BufferManagement.read(node, off);
					int searchOff = off + searchOrdinalPosition - 1;
					int searchCode = BufferManagement.read(node, searchOff);
					String columnString = "";
					if (searchOrdinalPosition > 1) {
						for (int i = 1; i < searchOrdinalPosition; i++) {
							int serialCode = BufferManagement.read(node, off);
							length = getLengthOfSerialCode(serialCode);
							loc = loc + length;
							off = off + 1;
						}
					}

					if (!(searchDataType.equalsIgnoreCase("text") && searchCode == 12)) {
						columnString = readColumn(node, searchDataType, length, loc);
					} else {
						columnString = null;
					}
					String columnDataType = SerialTypeCodes.getDataTypeForSerialCode(searchCode);
					boolean isSearchSuccess = search(searchQuery, searchDataType, columnString,columnDataType, operator);
					if (isSearchSuccess) {
						count = count + 1;
						loc = pointer + BtreeConstants.cellHeaderLength + noOfColumns
								+ BtreeConstants.lengthOfNoOfColumns;
						off = pointer + BtreeConstants.cellHeaderLength + BtreeConstants.lengthOfNoOfColumns;
						length = BufferManagement.read(node, off);

						// update datatype
						int ordOff = off + updateOrdinalPosition - 1;
						int sCode = BufferManagement.read(node, ordOff);
						String checkDataType = SerialTypeCodes.getDataTypeForSerialCode(sCode);
						if (!updateDataType.equalsIgnoreCase(checkDataType)) {
							sCode = SerialTypeCodes.getSerialCodeForDataType(updateDataType);
							node = BufferManagement.write(node, ordOff, (byte) sCode);
						}

						if (updateOrdinalPosition > 1) {
							for (int i = 1; i < updateOrdinalPosition; i++) {
								int serialCode = BufferManagement.read(node, off);
								length = getLengthOfSerialCode(serialCode);
								loc = loc + length;
								off = off + 1;
							}
						}
						node = writeColumn(node, updateDataType, updateValue, loc);
						tableFile.seek(pageNumber);
						tableFile.write(node, 0, (int) pageSize);
					}
				}
				pageNumber = BufferManagement.readInt(node, BtreeConstants.rightOffset);
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return count;
	}

	/**
	 * @param filename
	 * @param updateValue
	 * @param updateDataType
	 * @param updateOrdinalPosition
	 * @param pageSize
	 * @return count of records updated
	 */
	public static int updateAllRecordsInUserTable(String filename, String updateValue, String updateDataType,
			int updateOrdinalPosition, long pageSize) {

		RandomAccessFile tableFile;
		int count = 0;
		byte[] node = new byte[(int) pageSize];
		try {
			tableFile = new RandomAccessFile(filename, "rw");
			int pageNumber = FileUtils.getFirstLeafPageAddress(tableFile, pageSize);
			int loc = 0;
			int off = 0;
			int length = 0;
			while (pageNumber != -1) {
				tableFile.seek(pageNumber);
				tableFile.read(node, 0, (int) pageSize);
				int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
				int pointerArray[] = new int[noOfCells];
				int pointerLoc = BtreeConstants.pointerOffset;
				for (int i = 0; i < noOfCells; i++) {
					pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
					pointerLoc = pointerLoc + 2;
				}
				for (int pointer : pointerArray) {

					int noOfColumns = BufferManagement.read(node, pointer + BtreeConstants.cellHeaderLength);
					loc = pointer + BtreeConstants.cellHeaderLength + noOfColumns + BtreeConstants.lengthOfNoOfColumns;
					off = pointer + BtreeConstants.cellHeaderLength + BtreeConstants.lengthOfNoOfColumns;
					length = BufferManagement.read(node, off);

					// update datatype
					int ordOff = off + updateOrdinalPosition - 1;
					int sCode = BufferManagement.read(node, ordOff);
					String checkDataType = SerialTypeCodes.getDataTypeForSerialCode(sCode);
					if (!updateDataType.equalsIgnoreCase(checkDataType)) {
						sCode = SerialTypeCodes.getSerialCodeForDataType(updateDataType);
						node = BufferManagement.write(node, ordOff, (byte) sCode);
					}

					if (updateOrdinalPosition > 1) {
						for (int i = 1; i < updateOrdinalPosition; i++) {
							int serialCode = BufferManagement.read(node, off);
							length = getLengthOfSerialCode(serialCode);
							loc = loc + length;
							off = off + 1;
						}
					}
					node = writeColumn(node, updateDataType, updateValue, loc);
					count = count + 1;
				}
				tableFile.seek(pageNumber);
				tableFile.write(node, 0, (int) pageSize);
				pageNumber = BufferManagement.readInt(node, BtreeConstants.rightOffset);
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return count;
	}

	/**
	 * @param node
	 * @param dataType
	 * @param data
	 * @param offset
	 * @return page with column written
	 */
	public static byte[] writeColumn(byte[] node, String dataType, String data, int offset) {
		switch (dataType.toLowerCase()) {
		case "byte1null":
			node = BufferManagement.write(node, offset, (byte) 0);

			break;
		case "byte2null":
			node = BufferManagement.writeShort(node, offset, (short) 0);

			break;
		case "byte4null":
			node = BufferManagement.writeInt(node, offset, (int) 0);

			break;
		case "byte8null":
			node = BufferManagement.writeLong(node, offset, (long) 0);

			break;
		case "tinyint":
			node = BufferManagement.write(node, offset, Byte.parseByte(data));

			break;
		case "smallint":
			node = BufferManagement.writeShort(node, offset, Short.parseShort(data));

			break;
		case "int":
			node = BufferManagement.writeInt(node, offset, Integer.parseInt(data));

			break;
		case "bigint":
			node = BufferManagement.writeLong(node, offset, (long) Long.parseLong(data));

			break;
		case "real":
			node = BufferManagement.writeFloat(node, offset, (float) Float.parseFloat(data));

			break;
		case "double":
			node = BufferManagement.writeDouble(node, offset, (double) Double.parseDouble(data));

			break;
		case "datetime":
			node = BufferManagement.writeLong(node, offset, (long) Long.parseLong(data));
			break;
		case "date":
			node = BufferManagement.writeLong(node, offset, (long) Long.parseLong(data));
			break;
		case "text":
			node = BufferManagement.writeString(node, offset, data);
			break;
		}
		return node;
	}

	/**
	 * @param filename
	 * @param key
	 * @param pageSize
	 * @return page number of row id
	 */
	public static int getPageNumberForRowId(String filename, int key, long pageSize) {
		RandomAccessFile tableFile;
		int nodePage = -1;
		try {
			tableFile = new RandomAccessFile(filename, "rw");
			tableFile.seek(0);
			nodePage = tableFile.readInt();
			byte[] node = new byte[(int) (pageSize)];
			tableFile.seek(nodePage);
			tableFile.read(node, 0, (int) pageSize);

			int noOfCells = node[BtreeConstants.noOfCellsOffset];
			while ((int) node[0] != 13) {
				noOfCells = node[BtreeConstants.noOfCellsOffset];
				int firstPointer = (int) BufferManagement.readShort(node, BtreeConstants.pointerOffset);
				int leftMostNodeKey = BufferManagement.readInt(node, firstPointer + 4);
				int rightPointerLoc = noOfCells * BtreeConstants.pointer - BtreeConstants.pointer;
				int rightMostPointer = (int) BufferManagement.readShort(node,
						BtreeConstants.pointerOffset + rightPointerLoc);
				int rightMostNodeKey = BufferManagement.readInt(node, rightMostPointer + 4);
				if (key <= leftMostNodeKey) {
					nodePage = BufferManagement.readInt(node, firstPointer);
				} else if (key > rightMostNodeKey) {
					nodePage = BufferManagement.readInt(node, BtreeConstants.rightOffset);

				} else {
					int nextPointerLoc = BtreeConstants.pointerOffset + 2;
					for (int i = 1; i < noOfCells; i++) {
						int nextPointer = BufferManagement.readShort(node, nextPointerLoc);
						int nodeKey = BufferManagement.readInt(node, nextPointer + 4);
						if (key <= nodeKey) {
							nodePage = BufferManagement.readInt(node, nextPointer);
							break;
						}
						nextPointerLoc = nextPointerLoc + 2;

					}
				}
				tableFile.seek(nodePage);
				tableFile.read(node, 0, (int) pageSize);
			}

		} catch (Exception e) {
			System.out.println(e);
		}
		return nodePage;
	}

	/**
	 * @param filename
	 * @param key
	 * @param operator
	 * @param pageSize
	 * @return records matching rowid
	 */
	public static ArrayList<ArrayList<String>> getRecordFromUserTableWithRowid(String filename, int key,
			String operator, long pageSize) {
		ArrayList<ArrayList<String>> records = new ArrayList<ArrayList<String>>();
		RandomAccessFile tableFile;
		int pageNumber = 0;
		try {
			tableFile = new RandomAccessFile(filename, "rw");
			if (operator.equals("=")) {
				pageNumber = getPageNumberForRowId(filename, key, pageSize);
				int offset = 0;
				byte[] node = new byte[(int) pageSize];
				tableFile.seek(pageNumber);
				tableFile.read(node, 0, (int) pageSize);
				int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
				int pointerArray[] = new int[noOfCells];
				int pointerLoc = BtreeConstants.pointerOffset;
				for (int i = 0; i < noOfCells; i++) {
					pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
					pointerLoc = pointerLoc + 2;
				}
				for (int pointer : pointerArray) {

					offset = pointer + 2;

					int columnKey = BufferManagement.readInt(node, offset);
					if (key == columnKey) {
						records.add(readRecordFromNode(node, pointer));
						break;
					}
				}
			} else {
				int endKey = 0;
				switch (operator) {
				case "<":
					pageNumber = getFirstLeafPageAddress(tableFile, pageSize);
					endKey = key - 1;
					break;
				case "<=":
					pageNumber = getFirstLeafPageAddress(tableFile, pageSize);
					endKey = key;
					break;
				case ">":
					pageNumber = getPageNumberForRowId(filename, key + 1, pageSize);
					endKey = 2147483647;
					break;
				case ">=":
					pageNumber = getPageNumberForRowId(filename, key, pageSize);
					endKey = 2147483647;
					break;
				case "<>":
				case "!=":
					pageNumber = getFirstLeafPageAddress(tableFile, pageSize);
					endKey = 2147483647;
					break;
				}
				while (pageNumber != -1) {
					int offset = 0;
					byte[] node = new byte[(int) pageSize];
					tableFile.seek(pageNumber);
					tableFile.read(node, 0, (int) pageSize);
					int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
					int pointerArray[] = new int[noOfCells];
					int pointerLoc = BtreeConstants.pointerOffset;
					for (int i = 0; i < noOfCells; i++) {
						pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
						pointerLoc = pointerLoc + 2;
					}
					int columnKey = 0;
					for (int pointer : pointerArray) {
						offset = pointer + 2;
						columnKey = BufferManagement.readInt(node, offset);
						boolean isSearchSuccess = intSearch(key, columnKey, operator);
						if (isSearchSuccess) {
							records.add(readRecordFromNode(node, pointer));
						}
					}
					if (columnKey == endKey) {
						pageNumber = -1;
					} else {
						pageNumber = BufferManagement.readInt(node, BtreeConstants.rightOffset);
					}

				}
			}

		} catch (Exception e) {
			System.out.println(e);
		}
		return records;
	}

	/**
	 * @param filename
	 * @param key
	 * @param updateValue
	 * @param updateDataType
	 * @param updateOrdinalPosition
	 * @param operator
	 * @param pageSize
	 * @return count of records updated
	 */
	public static int updateRecordsInUserTableWithRowId(String filename, int key, String updateValue,
			String updateDataType, int updateOrdinalPosition, String operator, long pageSize) {

		RandomAccessFile tableFile;
		byte[] node = new byte[(int) pageSize];
		int count = 0;
		int pageNumber = 0;
		try {
			tableFile = new RandomAccessFile(filename, "rw");
			if (operator.equals("=")) {
				pageNumber = FileUtils.getPageNumberForRowId(filename, key, pageSize);
				int loc = 0;
				tableFile.seek(pageNumber);
				tableFile.read(node, 0, (int) pageSize);
				int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
				int pointerArray[] = new int[noOfCells];
				int pointerLoc = BtreeConstants.pointerOffset;
				for (int i = 0; i < noOfCells; i++) {
					pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
					pointerLoc = pointerLoc + 2;
				}
				for (int pointer : pointerArray) {

					int noOfColumns = BufferManagement.read(node, pointer + BtreeConstants.cellHeaderLength);
					loc = pointer + 2;

					int columnKey = BufferManagement.readInt(node, loc);
					if (columnKey == key) {
						count = count + 1;
						loc = pointer + BtreeConstants.cellHeaderLength + noOfColumns
								+ BtreeConstants.lengthOfNoOfColumns;
						int off = pointer + BtreeConstants.cellHeaderLength + BtreeConstants.lengthOfNoOfColumns;
						int length = BufferManagement.read(node, off);

						// update datatype
						int ordOff = off + updateOrdinalPosition - 1;
						int sCode = BufferManagement.read(node, ordOff);
						String checkDataType = SerialTypeCodes.getDataTypeForSerialCode(sCode);
						if (!updateDataType.equalsIgnoreCase(checkDataType)) {
							sCode = SerialTypeCodes.getSerialCodeForDataType(updateDataType);
							node = BufferManagement.write(node, ordOff, (byte) sCode);
						}

						if (updateOrdinalPosition > 1) {
							for (int i = 1; i < updateOrdinalPosition; i++) {
								int serialCode = BufferManagement.read(node, off);
								length = getLengthOfSerialCode(serialCode);
								loc = loc + length;
								off = off + 1;
							}
						}
						node = writeColumn(node, updateDataType, updateValue, loc);
						tableFile.seek(pageNumber);
						tableFile.write(node, 0, (int) pageSize);
					}
				}
			} else {
				int endKey = 0;
				switch (operator) {
				case "<":
					pageNumber = getFirstLeafPageAddress(tableFile, pageSize);
					endKey = key - 1;
					break;
				case "<=":
					pageNumber = getFirstLeafPageAddress(tableFile, pageSize);
					endKey = key;
					break;
				case ">":
					pageNumber = getPageNumberForRowId(filename, key + 1, pageSize);
					endKey = 2147483647;
					break;
				case ">=":
					pageNumber = getPageNumberForRowId(filename, key, pageSize);
					endKey = 2147483647;
					break;
				case "<>":
				case "!=":
					pageNumber = getFirstLeafPageAddress(tableFile, pageSize);
					endKey = 2147483647;
					break;
				}
				while (pageNumber != -1) {
					int loc = 0;
					tableFile.seek(pageNumber);
					tableFile.read(node, 0, (int) pageSize);
					int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
					int pointerArray[] = new int[noOfCells];
					int pointerLoc = BtreeConstants.pointerOffset;
					for (int i = 0; i < noOfCells; i++) {
						pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
						pointerLoc = pointerLoc + 2;
					}
					int columnKey = 0;
					for (int pointer : pointerArray) {

						int noOfColumns = BufferManagement.read(node, pointer + BtreeConstants.cellHeaderLength);
						loc = pointer + 2;

						columnKey = BufferManagement.readInt(node, loc);
						boolean isSearchSuccess = intSearch(key, columnKey, operator);
						if (isSearchSuccess) {
							count = count + 1;
							loc = pointer + BtreeConstants.cellHeaderLength + noOfColumns
									+ BtreeConstants.lengthOfNoOfColumns;
							int off = pointer + BtreeConstants.cellHeaderLength + BtreeConstants.lengthOfNoOfColumns;
							int length = BufferManagement.read(node, off);
							if (updateOrdinalPosition > 1) {
								for (int i = 1; i < updateOrdinalPosition; i++) {
									int serialCode = BufferManagement.read(node, off);
									length = getLengthOfSerialCode(serialCode);
									loc = loc + length;
									off = off + 1;
								}
							}
							node = writeColumn(node, updateDataType, updateValue, loc);
							tableFile.seek(pageNumber);
							tableFile.write(node, 0, (int) pageSize);
						}
					}
					if (columnKey == endKey) {
						pageNumber = -1;
					} else {
						pageNumber = BufferManagement.readInt(node, BtreeConstants.rightOffset);
					}
				}
			}

		} catch (Exception e) {
			System.out.println(e);
		}
		return count;
	}

	/**
	 * @param key
	 * @param columnKey
	 * @param operator
	 * @return is search success
	 */
	private static boolean intSearch(int key, int columnKey, String operator) {

		boolean isSuccess = false;
		switch (operator) {
		case "<":
			if (columnKey < key) {
				isSuccess = true;
			}
			break;
		case "<=":
			if (columnKey <= key) {
				isSuccess = true;
			}
			break;
		case ">":
			if (columnKey > key) {
				isSuccess = true;
			}
			break;
		case ">=":
			if (columnKey >= key) {
				isSuccess = true;
			}
			break;
		case "<>":
		case "!=":
			if (columnKey != key) {
				isSuccess = true;
			}
			break;
		}
		return isSuccess;
	}

	/**
	 * @param filename
	 * @param searchQuery
	 * @param pageSize
	 * @return isTablePresent
	 */
	public static boolean isTablePresent(String filename, String searchQuery, long pageSize) {

		boolean isPresent = false;
		RandomAccessFile tablesFile;
		try {
			tablesFile = new RandomAccessFile(filename, "rw");
			int pageNumber = FileUtils.getFirstLeafPageAddress(tablesFile, pageSize);
			int loc = 0;
			while (pageNumber != -1) {
				byte[] node = new byte[(int) pageSize];
				tablesFile.seek(pageNumber);
				tablesFile.read(node, 0, (int) pageSize);
				int noOfCells = BufferManagement.read(node, BtreeConstants.noOfCellsOffset);
				int pointerArray[] = new int[noOfCells];
				int pointerLoc = BtreeConstants.pointerOffset;
				for (int i = 0; i < noOfCells; i++) {
					pointerArray[i] = BufferManagement.readShort(node, pointerLoc);
					pointerLoc = pointerLoc + 2;
				}
				for (int pointer : pointerArray) {

					int noOfColumns = BufferManagement.read(node, pointer + BtreeConstants.cellHeaderLength);
					loc = pointer + BtreeConstants.cellHeaderLength + noOfColumns + BtreeConstants.lengthOfNoOfColumns;
					int off = pointer + BtreeConstants.cellHeaderLength + BtreeConstants.lengthOfNoOfColumns;
					int length = BufferManagement.read(node, off);

					String columnString = readColumn(node, "text", length, loc);
					int isActivePosition = 2;
					for (int i = 1; i < isActivePosition; i++) {
						int serialCode = BufferManagement.read(node, off);
						length = getLengthOfSerialCode(serialCode);
						loc = loc + length;
						off = off + 1;
					}
					String isActiveString = readColumn(node, "tinyint", length, loc);
					if (searchQuery.equalsIgnoreCase(columnString) && isActiveString.equals("1")) {
						isPresent = true;
						break;
					}

				}
				if (isPresent) {
					break;
				}
				pageNumber = BufferManagement.readInt(node, BtreeConstants.rightOffset);
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return isPresent;
	}

	/**
	 * @param datetime
	 * @return epoch of datetime
	 */
	public static long getDateTimeInEpoch(String datetime) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
		Date date;
		try {
			date = dateFormat.parse(datetime);
			Long epoch = date.getTime();
			return epoch;
		} catch (ParseException e) {

			e.printStackTrace();
			return 0;
		}

	}

	/**
	 * @param epoch
	 * @return datetime string for epoch
	 */
	public static String getDateTimeFromEpoch(long epoch) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
		Date date = new Date(epoch);
		String formattedDate = dateFormat.format(date);
		return formattedDate;
	}
}
