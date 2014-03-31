var gDays, gUsers, gStats;

var MS_PER_DAY = 1000 * 60 * 60 * 24;

var gSample = 0.05;

function sorted(l, comp) {
  l = l.slice();
  l.sort(comp);
  return l;
}

function triStateText(d) {
  switch (d) {
    case "1":
      return "Yes";
    case "0":
      return "No";
    }
  return "Unknown";
}

function dateAdd(d, ms) {
  return new Date(d.getTime() + ms);
}

d3.xhr("days.csv", "text/plain")
  .get()
  .on("load",
    function(t) {
      var daynest = d3.nest()
        .key(function(d) { return d[0]; })
        .key(function(d) { return d[1]; })
        .sortKeys(d3.descending)
        .rollup(function(week) {
          var l = [];
          week.forEach(function(day) {
            l[day[2]] = day[3] / gSample;
          });
          return l;
        });
      var days = daynest.map(d3.csv.parseRows(t.responseText,
        function(d, i) {
          return [d[0], d[1], numeric(d[2]), numeric(d[3])];
        }), d3.map);
      gDays = days;
      setupDays();
    })
  .on("error",
    function(a1, a2) {
      console.error(e);
      alert("Error fetching days.csv: " + e);
    });

function numeric(v) {
  return +v;
}

function currentChannel() {
  var r = d3.select("#channel-form [name=\"channel-selector\"]:checked").property("value");
  return r;
}

function setupDays() {
  if (!gDays) {
    return;
  }
  var byweek = gDays.get(currentChannel());

  var maxUsers = 0;
  var maxUserDays = 0;

  var data = [];

  var saturdays = sorted(byweek.keys(), d3.descending);
  saturdays.forEach(function(saturday) {
    var week = byweek.get(saturday);

    var users = 0;
    var userDays = 0;

    var weekdata = {
      saturday: saturday,
      users: [],
    };
    data.push(weekdata);

    for (var i = 1; i <= 7; ++i) {
      var count = week[i];
      if (count === undefined) {
        count = 0;
      }
      weekdata.users.push({
        n: i,
        users0: users,
        users1: users + count,
        days0: userDays,
        days1: userDays + count * i
      });
      users += count;
      userDays += count * i;
    }
    if (users > maxUsers) {
      maxUsers = users;
    }
    if (userDays > maxUserDays) {
      maxUserDays = userDays;
    }
  });

  var colors = ["#98abc5", "#8a89a6", "#7b6888", "#6b486b", "#a05d56", "#d0743c", "#ff8c00"];
  var legend = d3.select("#usersLegendList").text("");
  for (var i = colors.length - 1; i >= 0; --i) {
    legend.append("li").style("background-color", colors[i]).text(i + 1);
  }

  var color = d3.scale.ordinal()
    .range(colors).domain([1, 2, 3, 4, 5, 6, 7]);

  var height = 250;
  var width = 80;
  var margin = {
    top: 5,
    left: 50,
    right: 15,
    bottom: 130
  };

  var x = d3.scale.ordinal()
    .rangeRoundBands([width, 0], 0.2)
    .domain(saturdays);
  var y = d3.scale.linear()
    .rangeRound([0, height])
    .domain([maxUsers * 1.1, 0]);

  var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .ticks(5)
    .tickFormat(d3.format("s"));
  var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom");

  var svgg = d3.select("#users svg")
    .text("")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  svgg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + height + ")")
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
    .attr("y", height + margin.bottom - 5)
    .attr("x", width / 2)
    .attr("text-anchor", "middle")
    .text("Week Ending");


  var bar = svgg.selectAll(".bar")
    .data(data)
    .enter()
    .append("g")
    .attr("transform", function(d) { return "translate(" + x(d.saturday) + ",0)"; });

  bar.selectAll("rect")
    .data(function(d) { return d.users; })
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("width", x.rangeBand())
    .attr("y", function(d) { return y(d.users1); })
    .attr("height", function(d) { return y(d.users0) - y(d.users1); })
    .attr("fill", function(d) { return color(d.n); });

  y = d3.scale.linear()
    .rangeRound([0, height])
    .domain([maxUserDays * 1.1, 0]);

  yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .ticks(5)
    .tickFormat(d3.format("s"));

  svgg = d3.select("#usage svg")
    .text("")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  svgg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + height + ")")
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

  bar = svgg.selectAll(".bar")
    .data(data)
    .enter()
    .append("g")
    .attr("transform", function(d) { return "translate(" + x(d.saturday) + ",0)"; });

  bar.selectAll("rect")
    .data(function(d) { return d.users; })
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("width", x.rangeBand())
    .attr("y", function(d) { return y(d.days1); })
    .attr("height", function(d) { return y(d.days0) - y(d.days1); })
    .attr("fill", function(d) { return color(d.n); });
}

d3.xhr("users.csv", "text/plain")
  .get()
  .on("load",
    function(t) {
      var usernest = d3.nest()
        .key(function(d) { return d[0]; })
        .key(function(d) { return d[1]; })
        .key(function(d) { return d[2]; })
        .sortKeys(d3.ascending)
        .rollup(function(vl) {
          if (vl.length != 1) {
            throw Error("unexpected value");
          }
          return vl[0][3] / gSample;
        });
      var users = usernest.map(d3.csv.parseRows(t.responseText,
        function(d, i) {
          return [d[0], d[1], d[2], numeric(d[3])];
        }), d3.map);
      gUsers = users;
      setupUsers();
    })
  .on("error",
    function(e) {
      console.error(e);
      alert("Error fecthing users.csv: " + e);
    });

d3.selection.prototype.positionRect = function(x, y, width, height) {
  if (height < 0) {
    y += height;
    height = -height;
  }
  this.attr("x", x).attr("width", width)
      .attr("y", y).attr("height", height);
  return this;
}

function setupUsers() {
  if (!gUsers) {
    return;
  }
  var height = 300;
  var width = 150;
  var margin = {
    top: 35,
    bottom: 10,
    left: 50,
    right: 10
  };

  var channel = gUsers.get(currentChannel());

  var active = channel.get("active");
  var lost = channel.get("lost");
  var returning = channel.get("return");
  var newdata = channel.get("new");

  var activeCount = d3.sum(active.values());
  var lostCount = d3.sum(lost.values());
  var returningCount = d3.sum(returning.values());
  var newCount = d3.sum(newdata.values());

  var change = returningCount + newCount - lostCount;
  var maxCount = activeCount;
  if (change > 0) {
    maxCount += change;
  }
  var x = d3.scale.ordinal()
    .rangeRoundBands([0, width], 0.25)
    .domain(["active", "lost", "returning"]);

  var y = d3.scale.linear()
    .rangeRound([0, height])
    .domain([maxCount * 1.05, 0]);

  var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .ticks(10)
    .tickFormat(d3.format("s"));

  var svgg = d3.select("#retention svg")
    .text("")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  svgg.append("g")
    .attr("class", "y axis")
    .call(yAxis);
  svgg.append("g")
    .attr("class", "x axis")
    .append("line")
    .attr("x1", 0).attr("y1", height)
    .attr("x2", width).attr("y2", height);

  var colors = {
    "active": "#4C6185",
    "lost": "#DE2C1F",
    "returning": "#26967A",
    "new": "#A5E075"
  };
  for (var k in colors) {
    var s = d3.select("#retention-" + k);
    s.style("border-left", x.rangeBand() + "px solid " + colors[k]);
  }

  svgg.append("rect")
    .attr("class", "bar")
    .positionRect(x("active"), y(activeCount), x.rangeBand(), height - y(activeCount))
    .attr("fill", colors["active"]);

  var current = activeCount;

  svgg.append("line")
    .attr("class", "bar-connector")
    .attr("x1", x("active")).attr("x2", x("lost") + x.rangeBand())
    .attr("y1", y(current)).attr("y2", y(current));

  svgg.append("rect")
    .attr("class", "bar")
    .positionRect(x("lost"), y(current), x.rangeBand(), -y(lostCount) + height)
    .attr("fill", colors["lost"]);

  current -= lostCount;

  svgg.append("line")
    .attr("class", "bar-connector")
    .attr("x1", x("lost")).attr("x2", x("returning") + x.rangeBand())
    .attr("y1", y(current)).attr("y2", y(current));

  svgg.append("rect")
    .positionRect(x("returning"), y(current), x.rangeBand(), y(returningCount) - height)
    .attr("fill", colors["returning"]);

  current += returningCount;

  svgg.append("rect")
    .positionRect(x("returning"), y(current), x.rangeBand(), y(newCount) - height)
    .attr("fill", colors["new"]);

  svgg.append("text")
    .attr("x", width / 2)
    .text("\u0394 " + d3.format("+.2s")(change))
    .attr("text-anchor", "middle");

  height = 250;
  width = 500;
  var legendHeight = 60;
  var legendWidth = 200;
  margin = {
    top: 10,
    bottom: 40,
    left: 80,
    right: 10
  };

  var parser = d3.time.format.utc("%Y-%m-%d").parse;
  var data = newdata.entries().map(function(d) {
    return {
      date: parser(d.key),
      count: d.value
    };
  });
  data.sort(function(a, b) {
    return a.date.getTime() - b.date.getTime();
  });
  // simple 7-day moving average
  var rolling = [];
  data.forEach(function(d) {
    rolling.push(d.count);
    if (rolling.length > 7) {
      rolling.shift();
    }
    if (rolling.length == 7) {
      d.rolling = d3.mean(rolling);
    }
  });
  var maxNew = d3.max(data, function(d) { return d.count; });

  var startDate = data[0].date;
  var endDate = data.slice(-1)[0].date;

  var x = d3.time.scale()
    .range([0, width])
    .domain([dateAdd(startDate, MS_PER_DAY * -.5), dateAdd(endDate, MS_PER_DAY * .5)]);
  var y = d3.scale.linear()
    .rangeRound([0, height])
    .domain([maxNew * 1.05, 0]);

  var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom");
  yAxis = d3.svg.axis()
    .scale(y)
    .tickFormat(d3.format("0s"))
    .ticks(8)
    .orient("left");

  svgg = d3.select("#newbyday svg")
    .text("")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  // shade the weekends
  var weekends = [];
  for (var i = 0; ; ++i) {
    var d = new Date(endDate - MS_PER_DAY * i);
    if (d < startDate) {
      break;
    }
    if (d.getUTCDay() != 6 && d.getUTCDay() != 0) {
      continue;
    }
    weekends.push(d);
  }
  svgg.selectAll(".weekend")
    .data(weekends)
    .enter()
    .append("rect")
    .attr({
      "class": "weekend",
      x: function(d) { return x(dateAdd(d, MS_PER_DAY * -.5)); },
      width: x(dateAdd(startDate, MS_PER_DAY / 2)),
      y: 0,
      height: height
    });

  svgg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + height + ")")
    .call(xAxis);

  svgg.append("g")
    .attr("class", "y axis")
    .call(yAxis)
    .append("text");
  /*
    .attr("class", "label")
    .attr("transform", "translate(" + (-margin.left + 20) + "," + height / 2 +") rotate(-90)")
    .style("text-anchor", "middle")
    .text("New users per day");
  */

  var mainLine = d3.svg.line()
    .x(function(d) { return x(d.date); })
    .y(function(d) { return y(d.count); });
  var rollingLine = d3.svg.line()
    .x(function(d) { return x(d.date); })
    .y(function(d) { return y(d.rolling); });

  svgg.append("path")
    .datum(data)
    .attr("class", "line main")
    .attr("d", mainLine);

  var points = svgg.selectAll(".point")
    .data(data)
    .enter()
    .append("circle")
    .attr("class", "point main")
    .attr("cx", function(d) { return x(d.date); })
    .attr("cy", function(d) { return y(d.count); })
    .attr("r", 3);

  svgg.append("path")
    .datum(data.filter(function(d) { return d.rolling !== undefined; }))
    .attr("class", "line rolling")
    .attr("d", rollingLine);

  var legend = svgg.append("g")
    .attr("class", "legend")
    .attr("transform", "translate(15," + (height - legendHeight - 15) +")");
  legend.append("rect")
    .attr("class", "legend-outline")
    .attr({
      x: 0,
      y: 0,
      height: legendHeight,
      width: legendWidth
    });
  legend.append("line")
    .attr("class", "line main")
    .attr({
      x1: 10, x2: 35,
      y1: 20, y2: 20
    });
  legend.append("text")
    .attr({
      "class": "legend-label",
      x: 40, y: 20,
      "text-anchor": "start",
      "dominant-baseline": "middle"
    })
    .text("New Users");
  legend.append("line")
    .attr("class", "line rolling")
    .attr({
      x1: 10, x2: 35,
      y1: 40, y2: 40
    });
  legend.append("text")
    .attr({
      "class": "legend-label",
      x: 40, y: 40,
      "text-anchor": "start",
      "dominant-baseline": "middle"
    })
    .text("7-day rolling average");
}

d3.select("#channel-form").on("change",
  function() {
    setupDays();
    setupUsers();
    setupStats();
  });

d3.xhr("stats.csv", "text/plain")
  .get()
  .on("load",
    function(t) {
      var channelNest = d3.nest()
        .key(function(d) { return d.channel; });
      gStats = channelNest.map(d3.csv.parseRows(t.responseText,
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
            count: numeric(d[9]) / gSample
          };
        }), d3.map);
      setupStats();
    })
  .on("error",
    function(err) {
      console.error(err);
      alert("Error fetching stats.csv: " + e);
  });

function setupStats() {
  if (!gStats) {
    return;
  }

  var channelData = gStats.get(currentChannel());

  var defaultBrowserNest = d3.nest()
    .key(function(d) {
      switch (d.defaultBrowser) {
        case "1":
          return 1;
        case "0":
          return 0;
      }
      return "?";
    })
    .rollup(function(vl) {
      return d3.sum(vl, function(d) { return d.count; });
    });
  var defaultBrowser = defaultBrowserNest.map(channelData, d3.map).entries();
  defaultBrowser.sort(function(a, b) { return b.value - a.value; });
  var defaultTotal = d3.sum(defaultBrowser, function(d) { return d.value; });

  var height = 100;
  var width = 100;
  var radius = Math.min(width, height) / 2;

  var margin = {
    top: 5,
    left: 5,
    bottom: 5,
    right: 5
  };

  var colors = {
    "?": "#D294EB",
    "0": "#EB9F94",
    "1": "#94EB9F"
  };

  var svgg = d3.select("#defaultBrowser svg")
    .text("")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + (margin.left + width / 2) + "," +
          (margin.top + height / 2) + ")");

  var pie = d3.layout.pie()
    .sort(null)
    .value(function(d) { return d.value; });

  var g = svgg.selectAll(".arc")
    .data(pie(defaultBrowser))
    .enter()
    .append("g")
    .attr("class", "arc");

  var arc = d3.svg.arc()
    .outerRadius(radius).innerRadius(0);

  g.append("path")
    .attr("d", arc)
    .attr("fill", function(d) { return colors[d.data.key]; });

  var tr = d3.select("#defaultBrowser .legend tbody").selectAll("tr")
    .data(defaultBrowser, function(d) { return d.key; });

  var enter = tr.enter().append("tr");
  enter.append("td")
    .attr("class", "legend-color")
    .style("background-color", function(d) { return colors[d.key]; });
  enter.append("td")
    .attr("class", "legend-label")
    .text(function (d) { return triStateText(d.key); });
  enter.append("td")
    .attr("class", "legend-data");

  tr.select(".legend-data").text(function (d) {
    return d3.format(".1%")(d.value / defaultTotal);
  });

  var updateNest = d3.nest()
    .key(function(d) {
      switch (d.autoUpdate + "_" + d.updateEnabled) {
        case "1_1":
          return "Automatic";
        case "1_0":
          return "Enabled";
        case "0_0":
          return "Disabled";
      }
      return "Unknown";
    })
  .rollup(function(vl) {
    return d3.sum(vl, function(d) { return d.count; });
  });
  var updates = updateNest.map(channelData, d3.map);
  updates = [
    { key: "Automatic", value: updates.get("Automatic") },
    { key: "Enabled", value: updates.get("Enabled") },
    { key: "Disabled", value: updates.get("Disabled") },
    { key: "Unknown", value: updates.get("Unknown") }
  ];

  colors = {
    "Automatic": "#94EB9F",
    "Enabled": "#EBCC94",
    "Disabled": "#EB9F94",
    "Unknown": "#D294EB"
  };

  tr = d3.select("#updatesEnabled .legend tbody").selectAll("tr")
    .data(updates, function(d) { return d.key; });

  enter = tr.enter().append("tr");
  enter.append("td").attr("class", "legend-color")
    .style("background-color", function(d) { return colors[d.key]; });
  enter.append("td").attr("class", "updates-status")
    .text(function (d) { return d.key; });
  enter.append("td").attr("class", "updates-pct");

  tr.select(".updates-pct").text(function (d) {
    return d3.format(".1%")(d.value / defaultTotal);
  });

  svgg = d3.select("#updatesEnabled svg")
    .text("")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + (margin.left + width / 2) + "," +
          (margin.top + height / 2) + ")");

  pie = d3.layout.pie()
    .sort(null)
    .value(function(d) { return d.value; });

  g = svgg.selectAll(".arc")
    .data(pie(updates))
    .enter()
    .append("g")
    .attr("class", "arc")
    .append("path")
    .attr("d", arc)
    .attr("fill", function(d) { return colors[d.data.key]; });
}
