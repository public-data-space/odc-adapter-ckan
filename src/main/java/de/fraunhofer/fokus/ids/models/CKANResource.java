package de.fraunhofer.fokus.ids.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CKANResource {

    public String name;

    public String url;

    public String id;

    public String format;

    public String package_id;

    public String originalURL;
}
