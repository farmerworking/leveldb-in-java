package com.farmerworking.leveldb.in.java.data.structure.log;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ICRC32C;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.Assert.*;

public abstract class ILogTest {
    protected abstract ICRC32C getCrc32CImpl();

    protected abstract ICoding getCodingImpl();

    protected abstract ILogReader getLogReaderImpl(SequentialFile sequentialFile, ILogReporter logReporter, boolean checksum, long initialOffset);

    protected abstract ILogWriter getLogWriterImpl(WritableFile writableFile);

    protected abstract ILogWriter getLogWriterImpl(WritableFile writableFile, long offset);

    public static int[] initialOffsetRecordSizes = {
            10000, // Two sizable records in first block
            10000,
            2 * RecordType.kBlockSize - 1000, // Span three blocks
            1,
            13716, // Consume all but two bytes of block 3.
            RecordType.kBlockSize - RecordType.kHeaderSize // Consume the entirety of block 4.
    };

    public static long[] initialOffsetLastRecordOffsets = {
            0,
            RecordType.kHeaderSize + 10000,
            2 * (RecordType.kHeaderSize + 10000),
            2 * (RecordType.kHeaderSize + 10000) + (2 * RecordType.kBlockSize - 1000) + 3 * RecordType.kHeaderSize,
            2 * (RecordType.kHeaderSize + 10000) + (2 * RecordType.kBlockSize - 1000) + 3 * RecordType.kHeaderSize + RecordType.kHeaderSize + 1,
            3 * RecordType.kBlockSize
    };

    public static int numInitialOffsetRecords = initialOffsetLastRecordOffsets.length;

    // Construct a string of the specified length made out of the supplied
    // partial string.
    public static String bigString(String partial_string, int n) {
        StringBuilder result = new StringBuilder();
        while (result.length() < n) {
            result = result.append(partial_string);
        }
        return result.substring(0, n);
    }

    // Construct a string from a number
    public static String numberString(int n) {
        return String.valueOf(n);
    }

    private class StringDest implements WritableFile {
        StringBuilder builder = new StringBuilder();

        @Override
        public Status append(String data) {
            builder.append(data);
            return Status.OK();
        }

        public void replace(int index, char newChar) {
            builder.setCharAt(index, newChar);
        }

        public int getLength() {
            return builder.length();
        }

        public String getContent() {
            return builder.toString();
        }

        public void setContent(String content) {
            this.builder = new StringBuilder(content);
        }

        @Override
        public Status flush() { return Status.OK(); }
        @Override
        public Status close() { return Status.OK(); }
        @Override
        public Status sync() { return Status.OK(); }
    }

    private class StringSource implements SequentialFile {
        public String contents = "";
        public boolean forceError;
        public boolean returnedPartial;

        public StringSource() {
            this.forceError = false;
            this.returnedPartial = false;
        }

        @Override
        public Pair<Status, String> read(int n) {
            assertTrue("must not Read() after eof/error", !returnedPartial);

            if (forceError) {
                forceError = false;
                returnedPartial = true;
                return new Pair<>(Status.Corruption("read error"), "");
            }

            if (contents.length() < n) {
                n = contents.length();
                returnedPartial = true;
            }
            String result = contents.substring(0, n);
            contents = contents.substring(n);
            return new Pair<>(Status.OK(), result);
        }

        @Override
        public Status skip(long n) {
            if (forceError) {
                forceError = false;
                return Status.Corruption("skip error");
            }

            if (n > contents.length()) {
                contents = "";
                return Status.NotFound("in-memory file skipped past end");
            }

            contents = contents.substring((int) n);
            return Status.OK();
        }
    }

    private class ReportCollector implements ILogReporter {
        public long droppedBytes;
        public String message = "";

        public ReportCollector() {
            droppedBytes = 0;
        }

        @Override
        public void corruption(long bytes, Status status) {
            droppedBytes = droppedBytes + bytes;
            message = message + status.toString();
        }
    }

    private StringDest dest;
    private StringSource source;
    private ReportCollector report;
    private boolean reading;
    private ILogWriter writer;
    private ILogReader reader;

    public ILogTest() {
        dest = new StringDest();
        source = new StringSource();
        report = new ReportCollector();

        reading = false;
        writer = getLogWriterImpl(dest);
        reader = getLogReaderImpl(source, report, true, 0);
    }

    public void reopenForAppend() {
        writer = getLogWriterImpl(dest, dest.getLength());
    }

    public void write(String msg) {
        assertTrue("write() after starting to read", !reading);
        writer.addRecord(msg);
    }

    public String read() {
        if (!reading) {
            reading = true;
            source.contents = dest.getContent();
        }

        Pair<Boolean, String> readResult = reader.readRecord();
        if (readResult.getKey()) {
            return readResult.getValue();
        } else {
            return "EOF";
        }
    }

    void incrementByte(int offset, int delta) {
        char[] chars = dest.getContent().toCharArray();
        chars[offset] = (char)(chars[offset] + delta);
        dest.setContent(new String(chars));
    }

    void setByte(int offset, char new_byte) {
        dest.replace(offset, new_byte);
    }

    void shrinkSize(int bytes) {
        dest.setContent(dest.getContent().substring(0, dest.getLength()  - bytes));
    }

    void fixChecksum(int header_offset, int len) {
        // Compute crc of type/len/data
        char[] chars = new char[1 + len];
        dest.getContent().getChars(header_offset + 6, header_offset + 6 + 1 + len, chars, 0);
        byte[] bytes = new String(chars).getBytes(StandardCharsets.UTF_8);
        int crc = getCrc32CImpl().value(bytes, 0, bytes.length);
        crc = getCrc32CImpl().mask(crc);

        char[] newChars = dest.getContent().toCharArray();
        getCodingImpl().encodeFixed32(newChars, header_offset, crc);
        dest.setContent(new String(newChars));
    }

    void forceError() {
        source.forceError = true;
    }

    public long droppedBytes() {
        return report.droppedBytes;
    }

    public String reportMessage() {
        return report.message;
    }

    // Returns OK iff recorded error message contains "msg"
    String matchError(String msg) {
        if (!report.message.contains(msg)) {
            return report.message;
        } else {
            return "OK";
        }
    }

    public void writeInitialOffsetLog() {
        for (int i = 0; i < numInitialOffsetRecords; i++) {
            String record = StringUtils.repeat((char) (i + 'a'), initialOffsetRecordSizes[i]);
            write(record);
        }
    }

    void startReadingAt(int initial_offset) {
        reader = getLogReaderImpl(source, report, true/*checksum*/, initial_offset);
    }

    void checkOffsetPastEndReturnsNoRecords(int offset_past_end) {
        writeInitialOffsetLog();
        reading = true;
        source.contents = dest.getContent();
        ILogReader offset_reader = getLogReaderImpl(source, report, true, writtenBytes() + offset_past_end);
        Pair<Boolean, String> readResult = offset_reader.readRecord();
        assertTrue(!readResult.getKey());
    }

    public void checkInitialOffsetRecord(int initialOffset, int expectedRecordOffset) {
        writeInitialOffsetLog();
        reading = true;
        source.contents = dest.getContent();
        ILogReader offsetReader = getLogReaderImpl(source, report, true, initialOffset);

        // Read all records from expected_record_offset through the last one.
        assertTrue(expectedRecordOffset < numInitialOffsetRecords);
        for (; expectedRecordOffset < numInitialOffsetRecords; ++expectedRecordOffset) {
            Pair<Boolean, String> readResult = offsetReader.readRecord();
            assertTrue(readResult.getKey());
            String record = readResult.getValue();
            assertEquals(initialOffsetRecordSizes[expectedRecordOffset], record.length());
            assertEquals(initialOffsetLastRecordOffsets[expectedRecordOffset], offsetReader.lastRecordOffset());
            assertEquals((char)('a' + expectedRecordOffset), record.toCharArray()[0]);
        }
    }

    public int writtenBytes() {
        return dest.getLength();
    }

    @Test
    public void testEmptyRead() throws Exception {
        assertEquals("EOF", read());
    }

    @Test
    public void testWriteRead() throws Exception {
        write("foo");
        write("bar");
        write("");
        write("xxxx");
        assertEquals("foo", read());
        assertEquals("bar", read());
        assertEquals("", read());
        assertEquals("xxxx", read());
        assertEquals("EOF", read());
        assertEquals("EOF", read());  // Make sure reads at eof work
    }

    @Test
    public void testWriteReadManyBlocks() throws Exception {
        for (int i = 0; i < 100000; i++) {
            write(numberString(i));
        }

        for (int i = 0; i < 100000; i++) {
            assertEquals(numberString(i), read());
        }
        assertEquals("EOF", read());
    }

    @Test
    public void testWriteReadFragmentation() throws Exception {
        write("small");
        String medium = bigString("medium", 50000);
        String large = bigString("large", 100000);
        write(medium);
        write(large);
        assertEquals("small", read());
        assertEquals(medium, read());
        assertEquals(large, read());
        assertEquals("EOF", read());
    }

    @Test
    public void testMarginalTrailer() throws Exception {
        // Make a trailer that is exactly the same length as an empty record.
        int n = RecordType.kBlockSize - 2 * RecordType.kHeaderSize;
        write(bigString("foo", n));
        assertEquals(RecordType.kBlockSize - RecordType.kHeaderSize, writtenBytes());
        write("");
        write("bar");
        assertEquals(bigString("foo", n), read());
        assertEquals("", read());
        assertEquals("bar", read());
        assertEquals("EOF", read());
    }

    @Test
    public void testMarginalTrailer2() throws Exception {
        // Make a trailer that is exactly the same length as an empty record.
        int n = RecordType.kBlockSize - 2 * RecordType.kHeaderSize;
        write(bigString("foo", n));
        assertEquals(RecordType.kBlockSize - RecordType.kHeaderSize, writtenBytes());
        write("bar");
        assertEquals(bigString("foo", n), read());
        assertEquals("bar", read());
        assertEquals("EOF", read());
        assertEquals(0, droppedBytes());
        assertEquals("", reportMessage());
    }

    @Test
    public void testShortTrailer() throws Exception {
        int n = RecordType.kBlockSize - 2*RecordType.kHeaderSize + 4;
        write(bigString("foo", n));
        assertEquals(RecordType.kBlockSize - RecordType.kHeaderSize + 4, writtenBytes());
        write("");
        assertEquals(RecordType.kBlockSize + RecordType.kHeaderSize, writtenBytes());
        write("bar");
        assertEquals(bigString("foo", n), read());
        assertEquals("", read());
        assertEquals("bar", read());
        assertEquals("EOF", read());
    }

    @Test
    public void testAlignedEof() throws Exception {
        int n = RecordType.kBlockSize - 2*RecordType.kHeaderSize + 4;
        write(bigString("foo", n));
        assertEquals(RecordType.kBlockSize - RecordType.kHeaderSize + 4, writtenBytes());
        assertEquals(bigString("foo", n), read());
        assertEquals("EOF", read());
    }

    @Test
    public void testPadding() throws Exception {
        int n = RecordType.kBlockSize - 2 * RecordType.kHeaderSize + 2;
        write(bigString("foo", n));
        write("next");
        assertEquals(RecordType.kBlockSize + RecordType.kHeaderSize + 4, writtenBytes());
    }

    @Test
    public void testOpenForAppend() throws Exception {
        write("hello");
        reopenForAppend();
        write("world");
        assertEquals("hello", read());
        assertEquals("world", read());
        assertEquals("EOF", read());
    }

    @Test
    public void testRandomWriteRead() throws Exception {
        int N = 500;
        int[] randomSize = new int[N];
        Random random = new Random();
        for (int i = 0; i < N; i++) {
            randomSize[i] = random.nextInt(100);
            write(bigString(numberString(i), randomSize[i]));
        }
        for (int i = 0; i < N; i++) {
            assertEquals(bigString(numberString(i), randomSize[i]), read());
        }
        assertEquals("EOF", read());
    }

    // Tests of all the error paths in log_reader.cc follow:

    //========== readPhysicalRecord ==========
    @Test
    public void testReadError() throws Exception {
        write("foo");
        forceError();
        assertEquals("EOF", read());
        assertEquals(RecordType.kBlockSize, droppedBytes());
        assertEquals("OK", matchError("read error"));
    }

    @Test
    public void testTruncatedTrailingRecordIsIgnored() throws Exception {
        write("foo");
        shrinkSize(4);   // Drop all payload as well as a header byte
        assertEquals("EOF", read());
        // Truncated last record is ignored, not treated as an error.
        assertEquals(0, droppedBytes());
        assertEquals("", reportMessage());
    }

    @Test
    public void testBadLength() throws Exception {
        int kPayloadSize = RecordType.kBlockSize - RecordType.kHeaderSize;
        write(bigString("bar", kPayloadSize));
        write("foo");
        // Least significant size byte is stored in header[4].
        incrementByte(4, 1);
        assertEquals("foo", read());
        assertEquals(RecordType.kBlockSize, droppedBytes());
        assertEquals("OK", matchError("bad record length"));
    }

    @Test
    public void testBadLengthAtEndIsIgnored() throws Exception {
        write("foo");
        shrinkSize(1);
        assertEquals("EOF", read());
        assertEquals(0, droppedBytes());
        assertEquals("", reportMessage());
    }

    @Test
    public void testChecksumMismatch() throws Exception {
        write("foo");
        incrementByte(0, 10);
        assertEquals("EOF", read());
        assertEquals(10, droppedBytes());
        assertEquals("OK", matchError("checksum mismatch"));
    }

    @Test
    public void testChecksumMismatch2() throws Exception {
        write("foo");
        incrementByte(8, 10);
        assertEquals("EOF", read());
        assertEquals(10, droppedBytes());
        assertEquals("OK", matchError("checksum mismatch"));
    }

    //========== readRecord ==========
    @Test
    public void testSkipError() throws Exception {
        write("foo");
        startReadingAt(RecordType.kBlockSize + 10);
        forceError();
        assertEquals("EOF", read());
        assertEquals("OK", matchError("skip error"));
        assertEquals(RecordType.kBlockSize, droppedBytes());
    }

    @Test
    public void testBadRecordType() throws Exception {
        write("foo");
        // Type is stored in header[6]
        incrementByte(6, 100);
        fixChecksum(0, 3);
        assertEquals("EOF", read());
        assertEquals(3, droppedBytes());
        assertEquals("OK", matchError("unknown record type"));
    }

    @Test
    public void testUnexpectedMiddleType() throws Exception {
        write("foo");
        setByte(6, RecordType.kMiddleType.getValue());
        fixChecksum(0, 3);
        assertEquals("EOF", read());
        assertEquals(3, droppedBytes());
        assertEquals("OK", matchError("missing start"));
    }

    @Test
    public void testUnexpectedLastType() throws Exception {
        write("foo");
        setByte(6, RecordType.kLastType.getValue());
        fixChecksum(0, 3);
        assertEquals("EOF", read());
        assertEquals(3, droppedBytes());
        assertEquals("OK", matchError("missing start"));
    }

    @Test
    public void testUnexpectedFullType() throws Exception {
        write("foo");
        write("bar");
        setByte(6, RecordType.kFirstType.getValue());
        fixChecksum(0, 3);
        assertEquals("bar", read());
        assertEquals("EOF", read());
        assertEquals(3, droppedBytes());
        assertEquals("OK", matchError("partial record without end"));
    }

    @Test
    public void testUnexpectedFirstType() throws Exception {
        write("foo");
        write(bigString("bar", 100000));
        setByte(6, RecordType.kFirstType.getValue());
        fixChecksum(0, 3);
        assertEquals(bigString("bar", 100000), read());
        assertEquals("EOF", read());
        assertEquals(3, droppedBytes());
        assertEquals("OK", matchError("partial record without end"));
    }

    @Test
    public void testUnexpectedBadType() throws Exception {
        write("foo");
        write("bar");
        setByte(6, RecordType.kFirstType.getValue());
        fixChecksum(0, 3);
        incrementByte(10, 10);
        assertEquals("EOF", read());
        assertEquals(13, droppedBytes());
        assertEquals("OK", matchError("error in middle of record"));
    }

    @Test
    public void testMissingLastIsIgnored() throws Exception {
        write(bigString("bar", RecordType.kBlockSize));
        // Remove the LAST block, including header.
        shrinkSize(14);
        assertEquals("EOF", read());
        assertEquals("", reportMessage());
        assertEquals(0, droppedBytes());
    }

    @Test
    public void testPartialLastIsIgnored() throws Exception {
        write(bigString("bar", RecordType.kBlockSize));
        // Cause a bad record length in the LAST block.
        shrinkSize(1);
        assertEquals("EOF", read());
        assertEquals("", reportMessage());
        assertEquals(0, droppedBytes());
    }

    @Test
    public void testSkipIntoMultiRecord() throws Exception {
        // Consider a fragmented record:
        //    first(R1), middle(R1), last(R1), first(R2)
        // If initial_offset points to a record after first(R1) but before first(R2)
        // incomplete fragment errors are not actual errors, and must be suppressed
        // until a new first or full record is encountered.
        write(bigString("foo", 3 * RecordType.kBlockSize));
        write("correct");
        startReadingAt(RecordType.kBlockSize);

        assertEquals("correct", read());
        assertEquals("", reportMessage());
        assertEquals(0, droppedBytes());
        assertEquals("EOF", read());
    }

    @Test
    public void testErrorJoinsRecords() throws Exception {
        // Consider two fragmented records:
        //    first(R1) last(R1) first(R2) last(R2)
        // where the middle two fragments disappear.  We do not want
        // first(R1),last(R2) to get joined and returned as a valid record.

        // Write records that span two blocks
        write(bigString("foo", RecordType.kBlockSize));
        write(bigString("bar", RecordType.kBlockSize));
        write("correct");

        // Wipe the middle block
        for (int offset = RecordType.kBlockSize; offset < 2*RecordType.kBlockSize; offset++) {
            setByte(offset, 'x');
        }

        assertEquals("correct", read());
        assertEquals("EOF", read());
        long dropped = droppedBytes();
        assertTrue(dropped <= 2 * RecordType.kBlockSize + 100);
        assertTrue(dropped >= 2 * RecordType.kBlockSize);
    }

    @Test
    public void testReadStart() throws Exception {
        checkInitialOffsetRecord(0, 0);
    }

    @Test
    public void testReadSecondOneOff() throws Exception {
        checkInitialOffsetRecord(1, 1);
    }

    @Test
    public void testReadSecondTenThousand() throws Exception {
        checkInitialOffsetRecord(10000, 1);
    }

    @Test
    public void testReadSecondStart() throws Exception {
        checkInitialOffsetRecord(10007, 1);
    }

    @Test
    public void testReadThirdOneOff() throws Exception {
        checkInitialOffsetRecord(10008, 2);
    }

    @Test
    public void testReadThirdStart() throws Exception {
        checkInitialOffsetRecord(20014, 2);
    }

    @Test
    public void testReadFourthOneOff() throws Exception {
        checkInitialOffsetRecord(20015, 3);
    }

    @Test
    public void testReadFourthFirstBlockTrailer() throws Exception {
        checkInitialOffsetRecord(RecordType.kBlockSize - 4, 3);
    }

    @Test
    public void testReadFourthMiddleBlock() throws Exception {
        checkInitialOffsetRecord(RecordType.kBlockSize + 1, 3);
    }

    @Test
    public void testReadFourthLastBlock() throws Exception{
        checkInitialOffsetRecord(2 * RecordType.kBlockSize + 1, 3);
    }

    @Test
    public void testReadFourthStart() throws Exception {
        checkInitialOffsetRecord(
                2 * (RecordType.kHeaderSize + 1000) + (2 * RecordType.kBlockSize - 1000) + 3 * RecordType.kHeaderSize,
                3);
    }

    @Test
    public void testReadInitialOffsetIntoBlockPadding() throws Exception {
        checkInitialOffsetRecord(3 * RecordType.kBlockSize - 3, 5);
    }

    @Test
    public void testReadEnd() throws Exception {
        checkOffsetPastEndReturnsNoRecords(0);
    }

    @Test
    public void testReadPastEnd() throws Exception {
        checkOffsetPastEndReturnsNoRecords(5);
    }
}
