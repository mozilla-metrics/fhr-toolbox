var gDays = {};
var gLag, gDates;
var gReady = false;

var commaFormat = d3.format(",.0f");
function numeric(v) {
  return +v;
}
function adjustmentFactor(r, factor) {
  return (r + factor) / (factor + 1);
}
function dateAdd(d, ms) {
  return new Date(d.getTime() + ms);
}
var MS_PER_DAY = 1000 * 60 * 60 * 24;

function currentChannel() {
  var r = d3.select("#channel-form [name=\"channel-selector\"]:checked").property("value");
  return r;
}
function filterChannel(data) {
  var c = currentChannel();
  if (c == "all") {
    return data;
  }
  return data.filter(function(d) { return d.channel == c; });
}
function Dimensions(o) {
  if (!(this instanceof Dimensions)) {
    throw Error("Use new Dimensions()");
  }
  if (o !== undefined) {
    for (var k in o) {
      this[k] = o[k];
    }
  }
}
Dimensions.prototype.radius = function() {
  return Math.min(this.width, this.height) / 2;
};
Dimensions.prototype.totalWidth = function() {
  return this.width + this.marginLeft + this.marginRight;
};
Dimensions.prototype.totalHeight = function() {
  return this.height + this.marginTop + this.marginBottom;
};
Dimensions.prototype.transformUpperLeft = function(e) {
  e.attr("transform", "translate(" + this.marginLeft + "," + this.marginTop + ")");
};
Dimensions.prototype.transformCenter = function(e) {
  e.attr("transform", "translate(" + (this.marginLeft + this.width / 2) + "," +
         (this.marginTop + this.height / 2) + ")");
};
Dimensions.prototype.setupSVG = function(e) {
  e.attr({
    width: this.totalWidth(),
    height: this.totalHeight()
  });
};

var colors = d3.scale.category10();
function lighter(v) {
  var c = colors(v);
  var h = d3.hsl(c);
  var b = h.brighter();
  return b.toString();
}

function lagAdjustment(d) {
  for (var i = 0; i < gLag.data.length; ++i) {
    if (gLag.data[i].date >= d) {
      return (gLag.adjustedTotal - gLag.data[i].cumulative + gLag.data[i].adjusted) / gLag.adjustedTotal;
    }
  }
  return 1;
}

d3.json("dates.json", function(dates) {
  console.info("dates", dates);
  gDates = dates;
  var items = d3.select("#dateLabels").selectAll("li")
    .data(gDates)
    .enter()
    .append("li");
  items.append("span")
    .style({
      "display": "inline-block",
      "background-color": function(d) { return colors(d.date); },
      "width": "1em",
      "height": "1em",
      "margin-right": "5px"
    });
  items.append("span").text(
    function(d) {
      var r = d.date;
      if (d.sample != 1.0) {
        r += " (" + d3.format("%")(d.sample) + " sample)";
      }
      return r;
    });

  gDates.forEach(fetch);
  d3.xhr(gDates[0].date + "/pingdate.csv", "text/plain")
    .get()
    .on("load", function(xhr) {
      var lag = d3.csv.parseRows(xhr.responseText,
        function(d) {
          return {
            date: new Date(d[0]),
            count: numeric(d[1])
          };
        });
      gLag = processLag(lag, gDates[0].date);
      done();
  })
  .on("error", function() {
    console.error("Error fetching lag.csv");
  });
});

function fetch(dateobj) {
  d3.xhr(dateobj.date + "/days.csv", "text/plain")
    .get()
    .on("load", function(xhr) {
      gDays[dateobj.date] = d3.csv.parseRows(xhr.responseText,
        function(d) {
          if (d.length == 4) { // the 03-16 snapshot didn't include version
            d.splice(1, 0, "unknown");
          }
          return {
            channel: d[0],
            version: d[1],
            weekend: d[2],
            days: numeric(d[3]),
            count: numeric(d[4]) / dateobj.sample
          };
        });
    done();
  })
  .on("error", function() {
    console.error("Failed to fetch days.csv", d);
  });
}

function done() {
  if (!gLag) {
    return;
  }
  for (var i = 0; i < gDates.length; ++i) {
    if (!(gDates[i].date in gDays)) {
      return;
    }
  }
  gReady = true;
  graphIt();
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

function graphIt() {
  if (!gReady) {
    return;
  }
  var activeNest = d3.nest()
    .key(function(d) { return d.weekend; })
    .rollup(function(dlist) {
      var active = d3.sum(dlist, function(d) { return d.days == 0 ? 0 : d.count; });
      var days = d3.sum(dlist, function(d) { return d.count * d.days; });
      return {
        active: active,
        activeDays: days,
      };
    })
    .sortKeys(d3.ascending);

  var saturdays = d3.set();
  var maxActive = 0;

  var activeData = gDates.map(function(dobject, dateIndex) {
    var date = dobject.date;
    var activeByWeek = activeNest.entries(filterChannel(gDays[date]));
    if (dateIndex == 0) {
      activeByWeek.forEach(function(d) {
        var adjustment = adjustmentFactor(lagAdjustment(new Date(d.key)), 3);
        d.values.activeAdjusted = d.values.active / adjustment;
        d.values.activeDaysAdjusted = d.values.activeDays / adjustment;
      });
    }
    activeByWeek.forEach(function(d) {
      saturdays.add(d.key);
      maxActive = d3.max([d.values.active, d.values.activeAdjusted, maxActive]);
    });
    return {
      date: date,
      byWeek: activeByWeek
    };
  });

  saturdays = saturdays.values();
  saturdays.sort(d3.ascending);

  var dims = new Dimensions({
    width: 300,
    height: 250,
    marginTop: 5,
    marginLeft: 85,
    marginRight: 15,
    marginBottom: 130
  });

  var x = d3.scale.ordinal()
    .rangeRoundBands([0, dims.width], 0.2)
    .domain(saturdays);
  var y = d3.scale.linear()
    .rangeRound([0, dims.height])
    .domain([maxActive * 1.1, 0]);

  var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .ticks(5)
    .tickFormat(d3.format("s"));
  var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom");

  var svgg = d3.select("#users svg")
    .text("").call(dims.setupSVG.bind(dims))
    .append("g")
    .call(dims.transformUpperLeft.bind(dims));

  svgg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + dims.height + ")")
    .call(xAxis)
    .selectAll("text")
    .attr("y", 0)
    .attr("x", 9)
    .attr("dy", ".35em")
    .attr("transform", "rotate(90)")
    .style("text-anchor", "start");
  svgg.append("g")
    .attr("class", "y axis")
    .call(yAxis);

  svgg.append("text")
    .attr("y", dims.height + dims.marginBottom - 5)
    .attr("x", dims.width / 2)
    .attr("text-anchor", "middle")
    .text("Week Ending");

  var t = d3.transform();
  t.translate = [-dims.marginLeft + 15, dims.height / 2];
  t.rotate = -90;
  svgg.append("text")
    .attr("transform", t.toString())
    .attr("text-anchor", "middle")
    .text("Active Profiles");

  var line = d3.svg.line()
    .x(function(d) { return x(d.key); })
    .y(function(d) { return y(d.values.active); });

  var adjustedLine = d3.svg.line()
    .x(function(d) { return x(d.key); })
    .y(function(d) { return y(d.values.activeAdjusted); });

  var groups = svgg.selectAll(".dateGroup")
    .data(activeData)
    .enter()
    .append("g")
    .attr("class", "dateGroup")
    .attr("id", function(d) { return d.date; });

  groups.each(function(d, i) {
    var group = d3.select(this);
    if (i == 0) {
      group.append("path")
        .attr({
          "stroke": lighter(d.date),
          "stroke-dasharray": "5,3",
          "fill": "none",
          "d": adjustedLine(d.byWeek)
        });
      group.selectAll(".adjustedPoint")
        .data(d.byWeek)
        .enter()
        .append("circle")
        .attr({
          "class": "adjustedPoint",
          "cx": function(pd) { return x(pd.key); },
          "cy": function(pd) { return y(pd.values.activeAdjusted); },
          "r": 3,
          "fill": function(pd) { return lighter(d.date); },
          "title": function(pd) { return commaFormat(pd.values.activeAdjusted); }
        });
    }
    group.append("path")
      .attr({
        "stroke": colors(d.date),
        "stroke-width": 1,
        "fill": "none",
        d: line(d.byWeek)
      });
    group.selectAll(".point")
      .data(d.byWeek)
      .enter()
      .append("circle")
      .attr({
        "class": "point",
        "cx": function(pd) { return x(pd.key); },
        "cy": function(pd) { return y(pd.values.active); },
        "fill": colors(d.date),
        "r": 3,
        "title": function(pd) { return commaFormat(pd.values.active); }
      });
  });
}

d3.selectAll("#channel-form [name=\"channel-selector\"]").on("change", graphIt);
