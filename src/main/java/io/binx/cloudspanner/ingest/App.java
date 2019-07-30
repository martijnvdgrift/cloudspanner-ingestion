package io.binx.cloudspanner.ingest;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class App {

    /**
     * Class to contain Song data.
     */
    static class Song {

        final String songId;
        final String title;
        final int year;
        final int peak;

        Song(String songId, String title, int year, int peak) {
            this.songId = songId;
            this.title = title;
            this.year = year;
            this.peak = peak;
        }
    }

    private static final List<Song> SONGS =
            Arrays.asList(
                    new Song(getUUID(), "My Bonnie", 1961, 26),
                    new Song(getUUID(), "Love Me Do", 1962, 1),
                    new Song(getUUID(), "From Me to You", 1963, 116),
                    new Song(getUUID(), "She Loves You", 1963, 1),
                    new Song(getUUID(), "Roll Over Beethoven", 1963, 68),
                    new Song(getUUID(), "I Want to Hold Your Hand", 1963, 1),
                    new Song(getUUID(), "Please Please Me", 1964, 3),
                    new Song(getUUID(), "All My Loving", 1964, 45),
                    new Song(getUUID(), "Why", 1964, 88),
                    new Song(getUUID(), "Twist and Shout", 1964, 2),
                    new Song(getUUID(), "Can't Buy Me Love", 1964, 1),
                    new Song(getUUID(), "Do You Want to Know a Secret", 1964, 2),
                    new Song(getUUID(), "Ain't She Sweet", 1964, 19),
                    new Song(getUUID(), "A Hard Day's Night", 1964, 1),
                    new Song(getUUID(), "I'll Cry Instead", 1964, 25),
                    new Song(getUUID(), "And I Love Her", 1964, 12),
                    new Song(getUUID(), "Matchbox", 1964, 17),
                    new Song(getUUID(), "I Feel Fine", 1964, 1),
                    new Song(getUUID(), "Eight Days a Week", 1965, 1),
                    new Song(getUUID(), "Ticket to Ride", 1965, 1),
                    new Song(getUUID(), "Help", 1965, 1),
                    new Song(getUUID(), "Yesterday", 1965, 1),
                    new Song(getUUID(), "Boys", 1965, 102),
                    new Song(getUUID(), "We Can Work It Out", 1965, 1),
                    new Song(getUUID(), "Nowhere Man", 1966, 3),
                    new Song(getUUID(), "Paperback Writer", 1966, 1),
                    new Song(getUUID(), "Yellow Submarine", 1966, 2),
                    new Song(getUUID(), "Penny Lane", 1967, 1),
                    new Song(getUUID(), "All You Need Is Love", 1967, 1),
                    new Song(getUUID(), "Hello Goodbye", 1967, 1),
                    new Song(getUUID(), "Lady Madonna", 1968, 4),
                    new Song(getUUID(), "Hey Jude", 1968, 1),
                    new Song(getUUID(), "Get Back", 1969, 1),
                    new Song(getUUID(), "The Ballad of John and Yoko", 1969, 8),
                    new Song(getUUID(), "Something", 1969, 3),
                    new Song(getUUID(), "Let It Be", 1970, 1),
                    new Song(getUUID(), "The Long and Winding Road", 1970, 1),
                    new Song(getUUID(), "Got to Get You into My Life", 1976, 7),
                    new Song(getUUID(), "Ob-La-Di, Ob-La-Da", 1976, 49),
                    new Song(getUUID(), "Sgt. Pepper's Lonely Hearts Club Band", 1978, 71),
                    new Song(getUUID(), "The Beatles Movie Medley", 1982, 12),
                    new Song(getUUID(), "Baby It's You", 1995, 67),
                    new Song(getUUID(), "Free as a Bird", 1995, 6),
                    new Song(getUUID(), "Real Love", 1996, 11));

    private static void writeData(DatabaseClient dbClient) {
        List<Mutation> mutations = new ArrayList<>();
        for (Song song : SONGS) {
            mutations.add(
                    Mutation.newInsertBuilder("Songs")
                            .set("SongId")
                            .to(song.songId)
                            .set("Title")
                            .to(song.title)
                            .set("Year")
                            .to(song.year)
                            .set("Peak")
                            .to(song.peak)
                            .build());
        }
        dbClient.write(mutations);
    }

    public static void main(String[] args) {
        System.out.println("Ingesting into Spanner..");
        // Authenticating with Google Application Default Credentials
        try {
            GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();
        String instanceId = "cloudspanner-ingestion-lab";
        String databaseId = "demo";

        try {
            DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
            writeData(dbClient);
        } finally {
            spanner.close();
        }
    }

    private static String getUUID() {
        return java.util.UUID.randomUUID().toString();
    }
}