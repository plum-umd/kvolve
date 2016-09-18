#include <stdlib.h>
#include "kvolve_upd.h"

/* This is the update structure for amico 1.2 to 2.0 */
void kvolve_declare_update(){
    kvolve_upd_spec("amico:followers", "amico:followers:default", 12, 20, 0);
    kvolve_upd_spec("amico:following", "amico:following:default", 12, 20, 0);
    kvolve_upd_spec("amico:blocked", "amico:blocked:default", 12, 20, 0);
    kvolve_upd_spec("amico:reciprocated", "amico:reciprocated:default", 12, 20, 0);
    kvolve_upd_spec("amico:pending", "amico:pending:default", 12, 20, 0);
}
