package com.processor.gcpprocessor.utils;

import example.gcp.Client;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Serializator {
    private final String PATH = "src/main/resources/avro/client.avro";
    private final List<Client> clients;

    public Serializator(List<Client> clients) {
        this.clients = clients;
    }

    public void serialize() throws IOException {
        if (clients.size() > 0) {
            DataFileWriter<Client> clientDataFileWriter = new DataFileWriter<>(
                    new SpecificDatumWriter<>(Client.class)
            );

            File file = new File(PATH);

            clientDataFileWriter.create(clients.get(0).getSchema(), new File(PATH));
            for (Client client : clients) {
                clientDataFileWriter.append(client);
            }
            clientDataFileWriter.close();

            DataFileReader<Client> clientDataFileReader = new DataFileReader<>(file,new SpecificDatumReader<>(
                    Client.class));
            while (clientDataFileReader.hasNext()) {
                Client b1 = clientDataFileReader.next();
                System.out.println("deserialized from file: " + b1);
            }

        } else {
            System.out.println("Empty list");
        }
    }

    public static void main(final String[] args) throws IOException {

        List<Client> clients = new ArrayList<>();
        Client client = Client.newBuilder()
                .setId(12345)
                .setName("Eugen")
                .setAddress(null)  //NULLABLE
                .setPhone(null)  //NULLABLE
                .build();

        clients.add(client);

        Serializator serializator = new Serializator(clients);
        serializator.serialize();
    }
}
