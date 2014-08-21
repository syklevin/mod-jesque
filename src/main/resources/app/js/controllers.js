'use strict';

var pasiControllers = angular.module('pasiControllers', []);

pasiControllers.controller('PasiLoginCtrl', ['$scope', 'appSettings',
  function($scope, appSettings){

  }

])


pasiControllers.controller('PasiLobbyCtrl', ['$scope', 'pasiClient',
  function($scope, pasiClient){

  }

])

pasiControllers.controller('PasiRoomCtrl', ['$scope', '$routeParams', 'pasiClient',
  function($scope, $routeParams, pasiClient){

  }

]);