package de.fraunhofer.fokus.ids.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CKANDataset {

    public String id;

    public String title;

    public String name;

    public String notes;

    public String license_url;

    public String license_title;

    public String originalURL;

    public List<CKANTag> tags;

    public CKANOrganization organization;

    public String version;

}
