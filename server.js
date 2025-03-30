const express = require("express");
const mysql = require("mysql2/promise");
const admin = require("firebase-admin");

// Cargar credenciales de Firebase
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://firestore.googleapis.com/v1/projects/peakway-d58bb/databases/(default)/documents"
});

const db = mysql.createPool({
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0 ,
    host: "xbasets.synology.me",
    user: "usuario_externo",
    password: "P34kw4y.2025",
    database: "Firestore",
    port: 3306,
});

const app = express();
const PORT = 3002;

// Función para obtener datos de Firebase de las últimas 24 horas
async function getFirebaseData(collectionName) {
    const firestore = admin.firestore();
    const now = new Date();
    const yesterday = new Date(now.getTime() - (48 * 60 * 60 * 1000));
    let snapshot = null;
    console.log("Colección: ", collectionName);
    if(collectionName == "config") {
        snapshot = await firestore.collection(collectionName).get();
    }else{
        snapshot = await firestore.collection(collectionName)
        .where("Timestamp", ">=", yesterday)
        .get();
    }
    console.log("Snapshot: ", snapshot);
    let data = [];
    snapshot.forEach(doc => {
        data.push({ id: doc.id, ...doc.data() });
    });
    return data;
}

// Función para insertar en MySQL
async function insertIntoMySQL(collectionName, data) {
    if (data.length === 0) return "No hay datos nuevos para insertar.";

    const connection = await db.getConnection();
    
    try {
        let query;
        let values;

        switch(collectionName) {
            case 'syncronizations':
                query = `INSERT INTO syncronizations 
                        (id, 
                        InstallationId, 
                        ExceptionMessage,
                        Endpoint, 
                        AgentVersion,
                        Timestamp,
                        Successful,
                        EnabledCertificates,
                        TotalCertificates, 
                        IsFromPulling
                        ) 
                        VALUES ? 
                        ON DUPLICATE KEY UPDATE 
                        InstallationId=VALUES(installationId),
                        ExceptionMessage = VALUES(ExceptionMessage),
                        Endpoint = VALUES(Endpoint),
                        AgentVersion=VALUES(AgentVersion),
                        Timestamp=VALUES(Timestamp),
                        Successful = VALUES(Successful),
                        EnabledCertificates = VALUES(EnabledCertificates), 
                        TotalCertificates = VALUES(TotalCertificates),
                        IsFromPulling=VALUES(IsFromPulling)`
                        ;
                values = data.map(d => [
                    d.id,
                    d.InstallationId,
                    d.ExceptionMessage,
                    d.Endpoint,
                    d.AgentVersion,
                    convertTimestamp(d.Timestamp),
                    d.Successful,
                    d.EnabledCertificates,
                    d.TotalCertificates,
                    d.IsFromPulling,
                ]);
                break;
                case 'logins':
                query = `INSERT INTO logins 
                        (id, installationId, AgentVersion, IsFromPulling, Timestamp) 
                        VALUES ? 
                        ON DUPLICATE KEY UPDATE 
                        installationId=VALUES(installationId),
                        AgentVersion=VALUES(AgentVersion),
                        IsFromPulling=VALUES(IsFromPulling),
                        Timestamp=VALUES(Timestamp)`;
                values = data.map(d => [
                    d.id,
                    d.installationId,
                    d.AgentVersion,
                    d.IsFromPulling,
                    d.Timestamp
                ]);
                break;
            case 'config':
                query = `INSERT INTO config 
                        (id, ErrorLevel, AuditEnabled,AuditSyncEnabled, AuditLoginEnabled,AuditExceptionEnabled,
                        AuditAuditEnabled, AGENTENABLED, AuditLoginVaultEnabled,AuditGetDataVaultEnabled, AuditUpdateEnabled)
                        VALUES ?
                        ON DUPLICATE KEY UPDATE
                        ErrorLevel=VALUES(ErrorLevel),
                        AuditEnabled=VALUES(AuditEnabled),
                        AuditSyncEnabled=VALUES(AuditSyncEnabled),
                        AuditLoginEnabled=VALUES(AuditLoginEnabled),
                        AuditExceptionEnabled=VALUES(AuditExceptionEnabled),
                        AuditAuditEnabled=VALUES(AuditAuditEnabled),
                        AGENTENABLED=VALUES(AGENTENABLED),
                        AuditLoginVaultEnabled=VALUES(AuditLoginVaultEnabled),
                        AuditGetDataVaultEnabled=VALUES(AuditGetDataVaultEnabled),
                        AuditUpdateEnabled=VALUES(AuditUpdateEnabled)`;
                values = data.map(d => [
                    d.id,
                    d.ErrorLevel,
                    d.AuditEnabled,
                    d.AuditSyncEnabled,
                    d.AuditLoginEnabled,
                    d.AuditExceptionEnabled,
                    d.AuditAuditEnabled,
                    d.AGENTENABLED,
                    d.AuditLoginVaultEnabled,
                    d.AuditGetDataVaultEnabled,
                    d.AuditUpdateEnabled,
                ]);
                break;
            // Añadir casos para otras colecciones
            default:
                throw new Error(`Colección ${collectionName} no soportada`);
        }

        await connection.query(query, [values]);
        return `✅ Insertados ${data.length} registros en ${collectionName}`;
    } catch (error) {
        console.error("❌ Error insertando en MySQL:", error);
        throw error;
    } finally {
        connection.release();
    }
}

// Endpoint para actualizar una colección específica
app.get("/update/:collection", async (req, res) => {
    const collectionName = req.params.collection;

    try {
        const data = await getFirebaseData(collectionName);
        const result = await insertIntoMySQL(collectionName, data);
        res.send(result);
    } catch (error) {
        console.error("❌ Error en la actualización:", error);
        res.status(500).send("Error en la actualización.");
    }
});
// Función para convertir timestamp Firestore a formato MySQL
function convertTimestamp(timestamp) {
    if (timestamp && timestamp._seconds) {
      return new Date(timestamp._seconds * 1000)
        .toISOString()
        .slice(0, 19)
        .replace("T", " ");
    }
    return null;
  }
// Iniciar el servidor
app.listen(PORT, () => {
    console.log(`🚀 API corriendo en http://localhost:${PORT}`);
});
