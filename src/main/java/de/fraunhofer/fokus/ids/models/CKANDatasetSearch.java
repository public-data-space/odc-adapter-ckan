package de.fraunhofer.fokus.ids.models;

import java.io.Serializable;
import java.util.List;

public class CKANDatasetSearch implements Serializable {

    public String success;

    public Result result;


    public static class Result {

        public Integer count;

        public String sort;

        public List<Dataset> results;

    }

    public static class Dataset {

        public String id;

        public String title;

        public String name;

        public String notes;

        public String license_url;

        public String license_title;

        public List<Distribution> resources;

    }

    public static class Distribution {

        public String name;

        public String url;

        public String id;

        public String format;

    }

}
