// fake out d3
var document = {
  documentElement: {
    style: {}
  },
};
var window = {
  Element: function(){},
  CSSStyleDeclaration: function(){},
};
importScripts("d3.v3.min.js");

var MS_PER_DAY = 1000 * 60 * 60 * 24;
function numeric(v) {
  return +v;
}
function tristateBool(d) {
  switch (d) {
    case "True":
    case "1":
      return true;
    case "False":
    case "0":
      return false;
    case "?":
      return undefined;
  }
  throw Error("Unexpected tri-state value: " + d);
}
function dateAdd(d, ms) {
  return new Date(d.getTime() + ms);
}

self.onmessage = function(e) {
  var d = e.data;
  switch (d.type) {
    case "fetch":
      fetch(d.baseURI, d.snapshotDate);
      return;
  };
  throw Error("Unexpected message: " + e.data);
};

function fetch(baseURI, snapshotDate) {
  d3.xhr(baseURI + "/days.csv", "text/plain")
    .get()
    .on("load",
      function(t) {
        var days = d3.csv.parseRows(t.responseText,
          function(d) {
            if (d.length == 4) {
              d.splice(1, 0, "unknown");
            }
            return {
              channel: d[0],
              version: d[1],
              weekend: d[2],
              days: numeric(d[3]),
              count: numeric(d[4])
            };
          });
        self.postMessage({ type: "data-days", data: days });
      })
    .on("error",
      function(t) {
        console.error("error getting days.csv", t);
      });

  d3.xhr(baseURI + "/users.csv", "text/plain")
    .get()
    .on("load",
      function(t) {
        var users = d3.csv.parseRows(t.responseText,
          function(d, i) {
            return {
              channel: d[0],
              type: d[1],
              day: d[2],
              count: numeric(d[3])
            };
          });
        self.postMessage({ type: "data-users", data: users });
      })
    .on("error",
      function(t) {
        console.error("error getting days.csv", t);
      });

  d3.xhr(baseURI + "/stats.csv", "text/plain")
    .get()
    .on("load",
      function(t) {
        var stats = d3.csv.parseRows(t.responseText,
          function(d, i) {
            return {
              channel: d[0],
              version: d[1],
              locale: d[2],
              defaultBrowser: d[3],
              telemetry: d[4],
              autoUpdate: d[5],
              updateEnabled: d[6],
              geo: d[7],
              addonsv: d[8],
              count: numeric(d[9])
            };
          });
        self.postMessage({ type: "data-stats", data: stats });
      })
    .on("error",
      function(t) {
        console.error("error getting stats.csv", t);
    });

  d3.xhr(baseURI + "/pingdate.csv", "text/plain")
    .get()
    .on("load",
      function(t) {
        var lag = d3.csv.parseRows(t.responseText,
          function(d, i) {
            return {
              date: new Date(d[0]),
              count: numeric(d[1])
            };
          });
        self.postMessage({
          type: "data-lag",
          data: processLag(lag, snapshotDate)
        });
     })
    .on("error",
      function(t) {
        console.error("Error fetching pingdate.csv", t);
      });

  d3.xhr(baseURI + "/addons.csv", "text/plain")
    .get()
    .on("load",
      function(t) {
        var addons = d3.csv.parseRows(t.responseText,
          function(d, i) {
            return {
              channel: d[0],
              addonID: d[1],
              userDisabled: tristateBool(d[2]),
              appDisabled: tristateBool(d[3]),
              addonName: d[4],
              count: numeric(d[5])
            };
          });
        self.postMessage({ type: "data-addons", data: addons });
      })
      .on("error",
        function(t) {
          console.error("Error fetching addons.csv", t);
        });
}

function processLag(lag, snapshotDate) {
  var endDate = new Date(snapshotDate);
  var midPoint = dateAdd(endDate, -MS_PER_DAY * 60);
  var startDate = dateAdd(endDate, -MS_PER_DAY * 90);
  lag = lag.filter(function(d) {
    return d.date <= endDate && d.date > startDate;
  });
  lag.sort(function(a, b) { return d3.ascending(a.date, b.date); });

  // Assume that the daily loss from days 60-90 is relatively constant and use
  // that to calculate a daily loss rate.
  var dailyLag = d3.sum(lag,
    function(d) {
      if (d.date <= midPoint) {
        return d.count;
      }
      return 0;
    }) / 30;

  var total = d3.sum(lag, function(d) { return d.count; });

  var cumulative = 0;
  lag.forEach(function(d) {
    d.adjusted = d.count - dailyLag;
    cumulative += d.adjusted;
    d.cumulative = cumulative;
  });
  lag.adjustedTotal = cumulative;

  return {
    data: lag,
    startDate: startDate,
    endDate: endDate,
    total: total,
    adjustedTotal: cumulative,
    dailyLoss: dailyLag,
    dailyLossRatio: dailyLag / total
  };
}
