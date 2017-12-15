package com.github.Builder;

public enum ReaderType {
    XML("xml"),
    JSON("json");

    private final String extension;

    private ReaderType(String extension) {
        this.extension = extension;
    }

    public String getExtension() {
        return extension;
    }

    @Override
    public String toString() {

        return extension;
    }
}
