/*jshint globalstrict:true */
/*global angular:true */
'use strict';

/* controller.js */

angular.module('demo.controllers', [])
    .controller('SearchCtrl', function($scope, ejsResource) {

        //var ejs = ejsResource('http://localhost:9200');
        var ejs = ejsResource('http://portal.nimbusinformatics.com:9200');

        var QueryObj = ejs.QueryStringQuery().defaultField('Title');

        var activeFilters = {};

        var client = ejs.Request()
            .indices('stackoverflow')
            .types('question')
            .facet(
                ejs.TermsFacet('tags')
                    .field('Tags')
                    .size(10));

        $scope.isActive = function (field, term) {
            return activeFilters.hasOwnProperty(field + term);
        };

        var applyFilters = function(query) {

            var filter = null;
            var filters = Object.keys(activeFilters).map(function(k) { return activeFilters[k]; });

            if (filters.length > 1) {
                filter = ejs.AndFilter(filters);
            } else if (filters.length === 1) {
                filter = filters[0];
            }

            return filter ? ejs.FilteredQuery(query, filter) : query;
        };

        $scope.search = function() {
            $scope.results = client
                .query(applyFilters(QueryObj.query($scope.queryTerm || '*')))
                .doSearch();
        };

        $scope.filter = function(field, term) {
            if ($scope.isActive(field, term)) {
                delete activeFilters[field + term];
            } else {
                activeFilters[field + term] = ejs.TermFilter(field, term);
            }
            $scope.search(0);
        }

    });
