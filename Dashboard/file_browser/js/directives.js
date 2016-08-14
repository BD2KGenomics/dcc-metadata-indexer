/*jshint globalstrict:true */
/*global angular:true */
 
angular.module('demo.directives', [])
    .directive('bar', function() {
 
        return {
            restrict: 'E',
 
            scope: {
                onClick: '=',
                bind:    '=',
                field:   '@'
            },
 
            link: function(scope, element, attrs) {
 
                var width = 300;
                var height = 250;
 
                var x = d3.scale.linear().range([0, width]);
                var y = d3.scale.ordinal().rangeBands([0, height], .1);
 
                var svg = d3.select(element[0])
                    .append('svg')
                        .attr('preserveAspectRatio', 'xMaxYMin meet')
                        .attr('viewBox', '0 0 ' + (width + 75) + ' ' + height)
                        .append('g');
 
                scope.$watch('bind', function(data) {
 
                    if (data) {
 
                        x.domain([0, d3.max(data, function(d) { return d.count; })]);
                        y.domain(data.map(function(d) { return d.term; }));
 
                        var bars = svg.selectAll('rect')
                            .data(data, function(d, i) { return Math.random(); });
 
                        // d3 enter fn binds each new value to a rect
                        bars.enter()
                            .append('rect')
                                .attr('class', 'bar rect')
                                .attr('y', function(d) { return y(d.term); })
                                .attr('height', y.rangeBand())
                                .attr('width', function(d) { return x(d.count); });
 
                        // wire up event listeners - (registers filter callback)
                        bars.on('mousedown', function(d) {
                            scope.$apply(function() {
                                (scope.onClick || angular.noop)(scope.field, d.term);
                            });
                        });
 
                        // d3 exit/remove flushes old values (removes old rects)
                        bars.exit().remove();
 
                        var labels = svg.selectAll('text')
                            .data(data, function(d) { return Math.random(); });
 
                        labels.enter()
                            .append('text')
                                .attr('y', function(d) { return y(d.term) + y.rangeBand() / 2; })
                                .attr('x', function(d) { return x(d.count) + 3; })
                                .attr('dy', '.35em')
                                .attr('text-anchor', function(d) { return 'start'; })
                                .text(function(d) { return d.term + ' (' + d.count + ')'; });
 
                        // d3 exit/remove flushes old values (removes old rects)
                        labels.exit().remove();
                    } 
                })
            }
        };
    });
