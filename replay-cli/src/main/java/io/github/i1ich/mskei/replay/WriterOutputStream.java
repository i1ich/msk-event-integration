package io.github.i1ich.mskei.replay;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/** Tiny adapter: bytes written to a PrintStream end up in a PrintWriter (picocli's stdout). */
final class WriterOutputStream extends OutputStream {
    private final PrintWriter target;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    WriterOutputStream(PrintWriter target) {
        this.target = target;
    }

    @Override
    public void write(int b) {
        buffer.write(b);
        if (b == '\n') {
            flush();
        }
    }

    @Override
    public void flush() {
        if (buffer.size() > 0) {
            target.print(buffer.toString(StandardCharsets.UTF_8));
            buffer.reset();
        }
        target.flush();
    }
}
