Embulk::JavaPlugin.register_filter(
  "reverse_geocoding", "org.embulk.filter.reverse_geocoding.ReverseGeoCodingFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
