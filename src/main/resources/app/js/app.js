'use strict';

angular.module('pasiApp', [
  'pasiSettings',
  'pasiControllers',
  'pasiServices',
  // components
  'ngRoute',
  'ngAnimate'
])
.config(['$routeProvider',
  function($routeProvider){
    $routeProvider.when('/login', {
      templateUrl: 'partials/login.html',
      controller: 'PasiLobbyCtrl'
    }).
    when('/lobby', {
      templateUrl: 'partials/lobby.html',
      controller: 'PasiLobbyCtrl'
    }).
    when('/rooms/:roomType', {
      templateUrl: 'partials/room.html',
      controller: 'PasiRoomCtrl'
    }).
    otherwise({
      redirectTo: '/login'
    });
  }
]);
