package com.andreikubar.alfresco.migration.export.wireline;

public class Translation {

    private String translation;
    private String locale;

    public Translation(String translation, String locale) {
        this.translation = translation;
        this.locale = locale;
    }

    public String getTranslation() {
        return translation;
    }

    public String getLocale() {
        return locale;
    }

    @Override
    public String toString() {
        return "Translation{" +
                "translation='" + translation + '\'' +
                ", locale='" + locale + '\'' +
                '}';
    }
}
