#ifndef SORT_FILE_H
#define SORT_FILE_H

typedef enum SR_ErrorCode {
  SR_OK,
  SR_ERROR
} SR_ErrorCode;

typedef struct Record {
	int id;
	char name[15];
	char surname[20];
	char city[20];
} Record;

#define MAX_RECORDS (int)( (BF_BLOCK_SIZE-sizeof(int)) / RECORD_SIZE ) /* Max number of records we can store in a block. */
#define RECORD_SIZE (int)( sizeof(int) + 15 * sizeof(char) + 20 * sizeof(char) + 20 * sizeof(char) )

/*
 * Η συνάρτηση SR_Init χρησιμοποιείται για την αρχικοποίηση του sort_file.
 * Σε περίπτωση που εκτελεστεί επιτυχώς, επιστρέφεται SR_OK, ενώ σε
 * διαφορετική περίπτωση κάποιος κωδικός λάθους.
 */
SR_ErrorCode SR_Init();

/*
 * Η συνάρτηση SR_CreateFile χρησιμοποιείται για τη δημιουργία και
 * κατάλληλη αρχικοποίηση ενός άδειου αρχείου ταξινόμησης με όνομα fileName.
 * Σε περίπτωση που εκτελεστεί επιτυχώς, επιστρέφεται SR_OK, ενώ σε
 * διαφορετική περίπτωση κάποιος κωδικός λάθους.
 */
SR_ErrorCode SR_CreateFile(
	const char *fileName		/* όνομα αρχείου */
	);

/*
 * Η συνάρτηση SR_OpenFile ανοίγει το αρχείο με όνομα filename και διαβάζει
 * από το πρώτο μπλοκ την πληροφορία που αφορά το αρχείο ταξινόμησης. Επιστρέφει
 * στην μεταβλητή fileDesc τον αναγνωριστικό αριθμό ανοίγματος αρχείου, όπως
 * αυτός επιστράφηκε από το επίπεδο διαχείρισης μπλοκ. Σε περίπτωση που
 * εκτελεστεί επιτυχώς, επιστρέφεται SR_OK, ενώ σε διαφορετική περίπτωση
 * κάποιος κωδικός λάθους. Αν το αρχείο που ανοίχτηκε δεν πρόκειται για αρχείο
 * ταξινόμησης, τότε αυτό θεωρείται επίσης περίπτωση σφάλματος.
 */
SR_ErrorCode SR_OpenFile(
	const char *fileName, 		/* όνομα αρχείου */
  int *fileDesc             /* αναγνωριστικός αριθμός ανοίγματος αρχείου */
	);

/*
 * Η συνάρτηση SR_CloseFile κλείνει το αρχείο που προσδιορίζεται από τον
 * αναγνωριστικό αριθμό ανοίγματος fileDesc. Σε περίπτωση που εκτελεστεί
 * επιτυχώς, επιστρέφεται SR_OK, ενώ σε διαφορετική περίπτωση κάποιος
 * κωδικός λάθους.
 */
SR_ErrorCode SR_CloseFile(
	int fileDesc 		/* αναγνωριστικός αριθμός ανοίγματος αρχείου */
	);

/*
 * Η συνάρτηση SR_InsertEntry χρησιμοποιείται για την εισαγωγή μίας
 * εγγραφής στο αρχείο ταξινόμησης. Ο αναγνωριστικός αριθμός ανοίγματος του
 * αρχείου δίνεται με την fileDesc ενώ η εγγραφή προς εισαγωγή προσδιορίζεται
 * από τη δομή record. Η εγγραφή προστίθεται στο τέλος του αρχείου, μετά την
 * τρέχουσα τελευταία εγγραφή. Σε περίπτωση που εκτελεστεί επιτυχώς,
 * επιστρέφεται SR_OK, ενώ σε διαφορετική περίπτωση κάποιος κωδικός λάθους.
 */
SR_ErrorCode SR_InsertEntry(
	int fileDesc,		/* αναγνωριστικός αριθμός ανοίγματος αρχείου */
	Record record		/* δομή που προσδιορίζει την εγγραφή */
	);

/*
 * Η συνάρτηση αυτή ταξινομεί ένα BF αρχείο με όνομα input_​fileName ως προς το
 * πεδίο που προσδιορίζεται από το fieldNo χρησιμοποιώντας bufferSize block
 * μνήμης. Το​ fieldNo είναι ο αύξων αριθμός του πεδίου, δηλαδή αν το
 * fieldNo = 0, τότε το πεδίο ως προς το οποίο θέλουμε να κάνουμε ταξινόμηση
 * είναι το id, αν fieldNo = 1, τότε το πεδίο ως προς το οποίο θέλουμε να
 * ταξινομήσουμε είναι το name, κτλ. Η συνάρτηση επιστρέφει SR_OK σε περίπτωση
 * επιτυχίας, ενώ σε διαφορετική περίπτωση κάποιος κωδικός λάθους.
 * Πιο συγκεκριμένα, η λειτουργικότητα που πρέπει να υλοποιηθεί είναι η ακόλουθη:
 *
 *    * Να διαβάζονται οι εγγραφές από το αρχείο input_filename και να
 *      εισάγονται στο νέο αρχείο ταξινομημένες ως προς το πεδίο με αρίθμηση
 *      fieldNo. Η ταξινόμηση θα γίνει με βάση τον αλγόριθμο εξωτερικής
 *      ταξινόμησης (external sort).
 *
 *    * Ο αλγόριθμός θα πρέπει να εκμεταλλεύεται όλα τα block μνήμης που σας
 *      δίνονται από την μεταβλητή bufferSize και μόνον αυτά. Αν αυτά τα block
 *      είναι περισσότερα από BF_BUFFER_SIZE ή μικρότερα από 3 τότε θα
 *      επιστρέφεται κωδικός λάθους.
 */
SR_ErrorCode SR_SortedFile(
  const char* input_filename,   /* όνομα αρχείου προς ταξινόμηση */
  const char* output_filename,  /* όνομα του τελικού ταξινομημένου αρχείου */
  int fieldNo,                  /* αύξων αριθμός πεδίου προς ταξινόμηση */
  int bufferSize            /* Το πλήθος των block μνήμης που έχετε διαθέσιμα */
  );

/*
 * Η συνάρτηση SR_PrintAllEntries χρησιμοποιείται για την εκτύπωση όλων των
 * εγγραφών που υπάρχουν στο αρχείο ταξινόμησης. Το fileDesc είναι ο αναγνωριστικός
 * αριθμός ανοίγματος του αρχείου, όπως αυτός έχει επιστραφεί από το επίπεδο
 * διαχείρισης μπλοκ. Σε περίπτωση που εκτελεστεί επιτυχώς, επιστρέφεται SR_OK,
 * ενώ σε διαφορετική περίπτωση κάποιος κωδικός λάθους.
 */
SR_ErrorCode SR_PrintAllEntries(
  int fileDesc		/* αναγνωριστικός αριθμός ανοίγματος αρχείου */
  );

int compare(Record r1,Record r2,int fieldNo);

#endif // SORT_FILE_H
