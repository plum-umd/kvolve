Amico from:
* https://github.com/agoragames/amico/releases/tag/v1.2.0
* https://github.com/agoragames/amico/releases/tag/v2.0.0

Kvolve changes:

For amico:  (4 LoC)
lib/amico/relationships.rb (3):
+      def setns()
       Amico.redis.client.call(["client", "setname", "amico:followers@12,amico:following@12,amico:blocked@12,amico:reciprocated@12,amico:pending@12"])
       end
  

to the program startup (1):
+      Amico.setns


