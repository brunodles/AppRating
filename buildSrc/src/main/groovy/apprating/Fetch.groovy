package apprating

import com.brunodles.alchemist.Alchemist
import com.brunodles.alchemist.collectors.AttrCollector
import com.brunodles.alchemist.collectors.TextCollector
import com.brunodles.alchemist.nested.Nested
import com.brunodles.alchemist.selector.Selector
import com.brunodles.alchemist.usevalueof.UseValueOf
import com.google.api.core.ApiFuture
import com.google.auth.oauth2.GoogleCredentials
import com.google.firebase.FirebaseApp
import com.google.firebase.FirebaseOptions
import com.google.firebase.database.DatabaseReference
import com.google.firebase.database.FirebaseDatabase
import io.reactivex.Observable
import io.reactivex.schedulers.Schedulers

import java.text.SimpleDateFormat
import java.util.regex.Pattern

@SuppressWarnings("GroovyUnusedDeclaration")
class Fetch {

    static final idPattern = Pattern.compile(/\?id=(.*?)(?:&|$)/)

    private final Alchemist alchemist = new Alchemist()
    private final String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    private final DatabaseReference ratings
    private final Set<String> done = new HashSet<>()

    private final String[][] developers

    Fetch(String[][] developers) {
        this.developers = developers
        this.ratings = buildRatingsReference()
    }

    static DatabaseReference buildRatingsReference() {
        InputStream serviceAccount = new FileInputStream("google.json")
        FirebaseOptions options = new FirebaseOptions.Builder()
                .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                .setDatabaseUrl("https://brunodles-apprating.firebaseio.com/")
                .build()

        def firebase = FirebaseApp.initializeApp(options, "Admin-" + String.valueOf(Math.random().next().longValue()))
        return FirebaseDatabase.getInstance(firebase).getReference("ratings")
    }

    static void getApp(String[][] developers) {
        def disposable = new Fetch(developers)
                .appsMap()
                .subscribe(
                { r -> void },
                { e -> e.printStackTrace() }
        )

        while (!disposable.isDisposed())
            Thread.sleep(5000)
    }

    private Observable<ApiFuture<Void>> appsMap() {
        return Observable.fromArray(developers)
                .subscribeOn(Schedulers.computation())
                .map { developer -> Arrays.copyOfRange(developer, 1, developer.length) }
                .flatMap(Observable.&fromArray)
                .flatMap(this.&developerApps)
                .doOnNext { println "Prepare to send ${it.title()}" }
                .map(this.&mapToMap)
                .map { m -> ratings.child("${date}/${m.appId}".replaceAll(/[.#$\[\]]/, "-")).setValueAsync(m) }
    }

    private Observable<App> developerApps(String url) {
        return Observable.just(url)
                .map { it -> alchemist.parseUrl(it, Developer.class) }
                .flatMap { Observable.fromIterable(it.apps()) }
                .filter { App app -> !done.contains(app.detailsUrl()) }
                .doOnNext { m -> done.add(m.detailsUrl()) }
                .subscribeOn(Schedulers.newThread())
    }

    private Map<String, Object> mapToMap(App app) {
        def map = new HashMap<String, Object>()
//        def appRef = ratings.getReference("${date}/${empresa[0]}/${app.title()}")
        String appDetailsUrl = "https://play.google.com" + app.detailsUrl()

        map.put("name", app.title())
        map.put("coverImageUrl", "https" + app.converImage())
        map.put("url", appDetailsUrl)

        def matcher = idPattern.matcher(appDetailsUrl)
        if (!matcher.find())
            throw new RuntimeException("App id not found on \"${appDetailsUrl}\"")
        def group = matcher.group(1)
        if (group == null || group.isEmpty())
            throw new RuntimeException("App id not found on \"${appDetailsUrl}\"")
        map.put("appId", group)

        try {
            AppDetails appDetails = alchemist.parseUrl(appDetailsUrl, AppDetails.class)

            map.put("ratting", appDetails.rating())
            map.put("name", appDetails.name())
        } catch (Exception e) {
            new Exception("Failed to get details for \"${app.title()}\" using \"${appDetailsUrl}\".", e).printStackTrace()
        }
        return map
    }

    static String devId(String id) {
        return "https://play.google.com/store/apps/developer?id=$id"
    }

    static String cpl(String id) {
        return "https://play.google.com/store/apps/collection/cluster?clp=$id"
    }

    static String search(String query) {
        return "https://play.google.com/store/search?c=apps&q=$query"
    }

    static interface Developer {
        @Selector(".card-content")
        @Nested
        ArrayList<App> apps()
    }

    static interface App {
        @Selector(".cover-image")
        @AttrCollector("data-cover-large")
        String converImage();

        @Selector(".title")
        @AttrCollector("title")
        String title();

        @Selector(".card-click-target")
        @AttrCollector("href")
        String detailsUrl();
    }

    static interface AppDetails {
        @Selector("[itemprop=\"name\"] span")
        @TextCollector
        String name()

        @Selector("meta[itemprop=\"ratingValue\"]")
        @AttrCollector("content")
        @UseValueOf
        Float rating()
    }

}